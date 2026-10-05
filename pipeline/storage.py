"""Object storage behind one small interface: S3 (MinIO locally) and an in-memory fake for tests."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

import boto3
from botocore.config import Config

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

    from pipeline.config import Settings


class ObjectStore(Protocol):
    def list_keys(self, prefix: str) -> list[str]: ...
    def get(self, key: str) -> bytes: ...
    def put(self, key: str, data: bytes) -> None: ...
    def copy(self, src: str, dst: str) -> None: ...
    def delete_prefix(self, prefix: str) -> int: ...


class S3Store:
    def __init__(self, client: S3Client, bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    @classmethod
    def from_settings(cls, settings: Settings) -> S3Store:
        client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name="us-east-1",
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}),
        )
        return cls(client, settings.s3_bucket)

    def ensure_bucket(self) -> None:
        existing = {b["Name"] for b in self._client.list_buckets().get("Buckets", [])}
        if self._bucket not in existing:
            self._client.create_bucket(Bucket=self._bucket)

    def list_keys(self, prefix: str) -> list[str]:
        paginator = self._client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return sorted(keys)

    def get(self, key: str) -> bytes:
        return self._client.get_object(Bucket=self._bucket, Key=key)["Body"].read()

    def put(self, key: str, data: bytes) -> None:
        self._client.put_object(Bucket=self._bucket, Key=key, Body=data)

    def copy(self, src: str, dst: str) -> None:
        self._client.copy_object(
            Bucket=self._bucket, Key=dst, CopySource={"Bucket": self._bucket, "Key": src}
        )

    def delete_prefix(self, prefix: str) -> int:
        keys = self.list_keys(prefix)
        for key in keys:
            self._client.delete_object(Bucket=self._bucket, Key=key)
        return len(keys)


class MemoryStore:
    """Dict-backed store with the same behavior, for unit tests."""

    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects: dict[str, bytes] = dict(objects or {})

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(k for k in self.objects if k.startswith(prefix))

    def get(self, key: str) -> bytes:
        return self.objects[key]

    def put(self, key: str, data: bytes) -> None:
        self.objects[key] = data

    def copy(self, src: str, dst: str) -> None:
        self.objects[dst] = self.objects[src]

    def delete_prefix(self, prefix: str) -> int:
        keys = self.list_keys(prefix)
        for key in keys:
            del self.objects[key]
        return len(keys)
