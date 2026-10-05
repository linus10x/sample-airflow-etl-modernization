"""Runtime settings, read from the environment. Defaults match docker-compose.yml on localhost."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    s3_endpoint: str
    s3_bucket: str
    s3_access_key: str
    s3_secret_key: str
    warehouse_dsn: str
    slack_webhook_url: str | None

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            s3_endpoint=os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000"),
            s3_bucket=os.environ.get("S3_BUCKET", "vendor-drops"),
            s3_access_key=os.environ.get("S3_ACCESS_KEY", "demo-minio-user"),
            s3_secret_key=os.environ.get("S3_SECRET_KEY", "demo-minio-pass"),
            warehouse_dsn=os.environ.get(
                "WAREHOUSE_DSN",
                "postgresql://warehouse:warehouse@localhost:5433/warehouse",
            ),
            slack_webhook_url=os.environ.get("SLACK_WEBHOOK_URL") or None,
        )
