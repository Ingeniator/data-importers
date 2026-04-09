from __future__ import annotations

import base64
from typing import NamedTuple, Optional

import re

import structlog
from fastapi import Header, HTTPException

logger = structlog.get_logger(__name__)

_UNSAFE_PATH_RE = re.compile(r"[^a-zA-Z0-9_\-./]")


def _sanitize_key(raw: str) -> str:
    """Sanitize key: allow / for nesting, block traversal."""
    clean = raw.strip()
    # Block path traversal
    clean = clean.replace("..", "")
    # Replace unsafe chars
    clean = _UNSAFE_PATH_RE.sub("_", clean)
    # Collapse multiple slashes, strip leading/trailing
    clean = re.sub(r"/+", "/", clean).strip("/._-")
    return clean


class AuthContext(NamedTuple):
    public_key: str
    secret_key: str
    is_org_admin: bool = False


def get_auth(
    authorization: Optional[str] = Header(default=None),
    x_group_id: Optional[str] = Header(default=None),
    x_role: Optional[str] = Header(default=None),
) -> AuthContext:
    """Extract auth from X-Group-ID header (nginx) or Basic auth fallback."""
    is_org_admin = (x_role or "").upper() == "ORG_ADMIN"

    # Prefer group_id header forwarded by nginx (contains "tenant/user")
    if x_group_id:
        public_key = _sanitize_key(x_group_id)
        if not public_key:
            raise HTTPException(status_code=401, detail="Invalid group ID")
        logger.info("authenticated", public_key=public_key, is_org_admin=is_org_admin, source="header")
        return AuthContext(public_key=public_key, secret_key="", is_org_admin=is_org_admin)

    # Fallback: Basic auth (e.g. Langfuse SDK calling directly)
    if not authorization:
        logger.debug("auth_rejected", reason="no_authorization_header")
        raise HTTPException(status_code=401, detail="Missing authentication")
    if not authorization.startswith("Basic "):
        logger.debug("auth_rejected", reason="not_basic_auth", scheme=authorization.split(" ", 1)[0])
        raise HTTPException(status_code=401, detail="Missing authentication")

    try:
        decoded = base64.b64decode(authorization.removeprefix("Basic ")).decode()
        public_key, secret_key = decoded.split(":", 1)
    except Exception as e:
        logger.debug("auth_rejected", reason="malformed_credentials", error=str(e))
        raise HTTPException(status_code=401, detail="Malformed credentials")

    public_key = _sanitize_key(public_key)
    if not public_key:
        logger.debug("auth_rejected", reason="empty_public_key")
        raise HTTPException(status_code=401, detail="Empty credentials")

    logger.info("authenticated", public_key=public_key, source="basic")
    return AuthContext(public_key=public_key, secret_key=secret_key)
