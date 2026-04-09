"""Media read endpoint — retrieve media info + download URL (read-only)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import aioboto3
import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from dataimporter.auth import AuthContext, get_auth
from dataimporter.config import Datasource, Settings, get_settings
from dataimporter.s3 import _s3_client_config, _s3_session

logger = structlog.get_logger(__name__)

router = APIRouter()


class GetMediaResponse(BaseModel):
    mediaId: str
    contentType: str
    contentLength: int
    uploadedAt: datetime | None = None
    url: str
    urlExpiry: str


def _resolve_s3_datasource(
    datasource: str = Query(),
    settings: Settings = Depends(get_settings),
) -> Datasource:
    ds = settings.get_datasource(datasource)
    if ds is None:
        raise HTTPException(status_code=404, detail=f"Datasource '{datasource}' not found")
    if ds.type != "s3":
        raise HTTPException(status_code=400, detail=f"Datasource '{datasource}' is not an S3 datasource")
    return ds


def _meta_key(public_key: str, media_id: str, ds: Datasource) -> str:
    prefix = f"{public_key}/media/{media_id}"
    if ds.key_prefix:
        prefix = f"{ds.key_prefix.strip('/')}/{prefix}"
    return prefix + ".meta.json"


def _blob_key(public_key: str, media_id: str, ds: Datasource) -> str:
    prefix = f"{public_key}/media/{media_id}"
    if ds.key_prefix:
        prefix = f"{ds.key_prefix.strip('/')}/{prefix}"
    return prefix + ".bin"


@router.get("/api/public/media/{media_id}", response_model=GetMediaResponse)
async def get_media(
    media_id: str,
    ds: Datasource = Depends(_resolve_s3_datasource),
    auth: AuthContext = Depends(get_auth),
) -> GetMediaResponse:
    meta = _meta_key(auth.public_key, media_id, ds)
    blob = _blob_key(auth.public_key, media_id, ds)

    session = _s3_session(ds)
    async with session.client("s3", endpoint_url=ds.endpoint, config=_s3_client_config(ds)) as client:
        try:
            obj = await client.get_object(Bucket=ds.bucket, Key=meta)
        except client.exceptions.NoSuchKey:
            raise HTTPException(status_code=404, detail="Media not found")
        except client.exceptions.ClientError as e:
            if int(e.response["Error"].get("Code", 0)) == 404:
                raise HTTPException(status_code=404, detail="Media not found")
            raise

        meta_body = json.loads(await obj["Body"].read())

        download_url = await client.generate_presigned_url(
            "get_object",
            Params={"Bucket": ds.bucket, "Key": blob},
            ExpiresIn=ds.presign_expiry,
        )
        if ds.public_endpoint and ds.endpoint:
            download_url = download_url.replace(ds.endpoint, ds.public_endpoint, 1)

    expiry = datetime.now(timezone.utc) + timedelta(seconds=ds.presign_expiry)

    uploaded_at_raw = meta_body.get("uploadedAt")
    uploaded_at = None
    if uploaded_at_raw:
        uploaded_at = datetime.fromisoformat(uploaded_at_raw)

    return GetMediaResponse(
        mediaId=media_id,
        contentType=meta_body["contentType"],
        contentLength=meta_body["contentLength"],
        uploadedAt=uploaded_at,
        url=download_url,
        urlExpiry=expiry.isoformat(),
    )
