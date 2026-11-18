"""Raw payload archive in MinIO / S3. Disabled when MINIO_ENDPOINT is empty."""
import asyncio
import io
import json
import logging
from datetime import datetime, timezone

from app.config import settings

log = logging.getLogger(__name__)


class RawStore:
    def __init__(self) -> None:
        self.client = None
        if not settings.minio_endpoint:
            log.info("MinIO disabled (MINIO_ENDPOINT not set)")
            return
        from minio import Minio

        self.client = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=False,
        )
        if not self.client.bucket_exists(settings.minio_bucket):
            self.client.make_bucket(settings.minio_bucket)

    async def put(self, url_hash: str, payload: dict) -> str | None:
        if self.client is None:
            return None
        now = datetime.now(timezone.utc)
        key = f"raw/{now:%Y/%m/%d}/{url_hash}.json"
        body = json.dumps(payload, ensure_ascii=False, default=str).encode()
        await asyncio.to_thread(
            self.client.put_object,
            settings.minio_bucket, key, io.BytesIO(body), len(body), content_type="application/json",
        )
        return key
