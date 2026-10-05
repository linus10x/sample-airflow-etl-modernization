"""Fixtures for tests that need the compose Postgres and MinIO (`make test` starts them).

Each session gets its own database and bucket, so nothing here can touch the demo data.
Set REQUIRE_SERVICES=1 (CI does) to fail instead of skip when the services are not reachable.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import psycopg
import pytest
from psycopg import conninfo

from pipeline.config import Settings
from pipeline.feeds import FEEDS
from pipeline.ingest import IngestResult, ingest_feed
from pipeline.storage import S3Store
from pipeline.warehouse import Warehouse

ROOT = Path(__file__).resolve().parent.parent


def _base_settings() -> Settings:
    return Settings.from_env()


def _admin_dsn(base_dsn: str) -> str:
    return conninfo.make_conninfo(base_dsn, dbname="postgres")


def _dsn_for(base_dsn: str, dbname: str) -> str:
    return conninfo.make_conninfo(base_dsn, dbname=dbname)


def _unavailable(reason: str) -> None:
    if os.environ.get("REQUIRE_SERVICES") == "1":
        pytest.fail(f"services required but not reachable: {reason}")
    pytest.skip(f"compose services not reachable ({reason}); run `make up`")


@pytest.fixture(scope="session")
def base_settings() -> Settings:
    settings = _base_settings()
    try:
        with psycopg.connect(_admin_dsn(settings.warehouse_dsn), connect_timeout=3):
            pass
    except psycopg.OperationalError as exc:
        _unavailable(f"postgres: {exc}")
    try:
        S3Store.from_settings(settings).list_keys("probe/")
    except Exception as exc:  # noqa: BLE001
        # The bucket may not exist yet; that is fine. A refused connection is not.
        if "NoSuchBucket" not in str(exc):
            _unavailable(f"s3: {exc}")
    return settings


def create_database(base_dsn: str, name: str) -> str:
    with psycopg.connect(_admin_dsn(base_dsn), autocommit=True) as conn:
        conn.execute(f'drop database if exists "{name}" with (force)')  # type: ignore[call-overload]
        conn.execute(f'create database "{name}"')  # type: ignore[call-overload]
    return _dsn_for(base_dsn, name)


def drop_database(base_dsn: str, name: str) -> None:
    with psycopg.connect(_admin_dsn(base_dsn), autocommit=True) as conn:
        conn.execute(f'drop database if exists "{name}" with (force)')  # type: ignore[call-overload]


@dataclass(frozen=True)
class Stack:
    settings: Settings
    warehouse: Warehouse
    store: S3Store
    db_name: str


def _make_stack(base: Settings, prefix: str) -> Iterator[Stack]:
    tag = uuid.uuid4().hex[:8]
    db_name = f"{prefix}_{tag}"
    bucket = f"{prefix}-{tag}".replace("_", "-")
    dsn = create_database(base.warehouse_dsn, db_name)
    settings = Settings(base.s3_endpoint, bucket, base.s3_access_key, base.s3_secret_key, dsn, None)
    store = S3Store.from_settings(settings)
    store.ensure_bucket()
    warehouse = Warehouse(dsn)
    warehouse.ensure_schema()
    try:
        yield Stack(settings, warehouse, store, db_name)
    finally:
        store.delete_prefix("")
        store._client.delete_bucket(Bucket=bucket)  # noqa: SLF001
        drop_database(base.warehouse_dsn, db_name)


@pytest.fixture
def fresh_stack(base_settings: Settings) -> Iterator[Stack]:
    """An empty database and bucket for one test."""
    yield from _make_stack(base_settings, "it")


@pytest.fixture(scope="session")
def dataset(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out = tmp_path_factory.mktemp("dataset")
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "generate.py"),
            "--seed",
            "42",
            "--out",
            str(out),
            "--overrides-out",
            str(out / "sku_overrides.csv"),
        ],
        check=True,
        capture_output=True,
    )
    return out


@pytest.fixture(scope="session")
def facts(dataset: Path) -> dict[str, object]:
    return json.loads((dataset / "FACTS.json").read_text())  # type: ignore[no-any-return]


def upload_dataset(store: S3Store, dataset: Path) -> int:
    count = 0
    for path in sorted((dataset / "incoming").rglob("*.json")):
        store.put(path.relative_to(dataset).as_posix(), path.read_bytes())
        count += 1
    return count


@dataclass(frozen=True)
class LoadedStack:
    stack: Stack
    results: dict[str, IngestResult]


@pytest.fixture(scope="session")
def loaded_stack(base_settings: Settings, dataset: Path) -> Iterator[LoadedStack]:
    """The full 30 day dataset, uploaded and ingested once, shared by read-only tests."""
    gen = _make_stack(base_settings, "loaded")
    stack = next(gen)
    try:
        upload_dataset(stack.store, dataset)
        results = {
            feed: ingest_feed(stack.store, stack.warehouse, feed, run_id="fixture")
            for feed in FEEDS
        }
        yield LoadedStack(stack, results)
    finally:
        next(gen, None)
