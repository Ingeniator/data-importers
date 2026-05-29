# Feature: Authentication

## One-liner
Two-method auth: nginx-forwarded `X-Group-ID` header (primary) or HTTP Basic credentials (fallback).

## Problem
The service is deployed behind nginx as a multi-tenant system. Each tenant is identified by a group ID injected by nginx. Direct API callers (SDKs, curl) use HTTP Basic auth.

## Implementation
- `src/dataimporter/auth.py` — `get_auth()` FastAPI dependency.
- `X-Group-ID` header value becomes `public_key` for tenant-scoped S3 key lookups.
- `X-Role: ORG_ADMIN` header sets `is_org_admin=True` on the auth context.
- Basic auth: `base64(public_key:secret_key)` — path-traversal sanitized before use.
- `AuthContext(public_key, secret_key, is_org_admin)` is a `NamedTuple` passed to all adapter calls.

## Scope
- **In**: Header auth, Basic auth, path-traversal sanitization, org-admin flag.
- **Out**: JWT/OAuth2 bearer tokens; per-user RBAC beyond the org-admin flag.

## Known gaps
- No auth on proxy routes (by design — proxy enforces URL allowlist instead).
- `is_org_admin` flag is extracted but not yet enforced by any route.
