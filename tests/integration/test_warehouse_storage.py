from __future__ import annotations

import threading
from datetime import date
from typing import Any

import pytest

from pipeline.storage import S3Store
from tests.services import Stack

DAY = date(2026, 3, 1)


def load(stack: Stack, checksum: str, records: list[Any], feed: str = "vendor_a") -> bool:
    return stack.warehouse.record_loaded(
        feed=feed,
        path=f"incoming/{feed}/{DAY}.json",
        checksum=checksum,
        file_date=DAY,
        contract_version="v1",
        records=records,
        size_bytes=10,
        run_id="t",
    )


def count(stack: Stack, table: str) -> int:
    return int(stack.warehouse.fetch_all(f"select count(*) as n from {table}")[0]["n"])  # noqa: S608


@pytest.mark.integration
def test_ensure_schema_is_idempotent(fresh_stack: Stack) -> None:
    fresh_stack.warehouse.ensure_schema()
    fresh_stack.warehouse.ensure_schema()
    tables = {
        r["table_name"]
        for r in fresh_stack.warehouse.fetch_all(
            "select table_name from information_schema.tables where table_schema in ('raw','ops')"
        )
    }
    assert tables == {"records", "file_manifest"}


@pytest.mark.integration
def test_loaded_file_writes_manifest_and_every_record(fresh_stack: Stack) -> None:
    assert load(fresh_stack, "c1", [{"sku": "AC-1"}, {"sku": "AC-2"}]) is True
    assert count(fresh_stack, "raw.records") == 2
    assert fresh_stack.warehouse.status_of("c1") == "loaded"
    assert fresh_stack.warehouse.status_of("missing") is None
    row = fresh_stack.warehouse.fetch_all(
        "select row_count, contract_version from ops.file_manifest"
    )[0]
    assert (row["row_count"], row["contract_version"]) == (2, "v1")


@pytest.mark.integration
def test_same_checksum_is_refused_and_writes_nothing(fresh_stack: Stack) -> None:
    assert load(fresh_stack, "c1", [{"a": 1}]) is True
    assert load(fresh_stack, "c1", [{"a": 1}, {"a": 2}]) is False
    assert count(fresh_stack, "raw.records") == 1
    assert count(fresh_stack, "ops.file_manifest") == 1


@pytest.mark.integration
def test_racing_workers_cannot_both_load_the_same_file(fresh_stack: Stack) -> None:
    results: list[bool] = []
    lock = threading.Lock()

    def worker() -> None:
        ok = load(fresh_stack, "race", [{"n": i} for i in range(50)])
        with lock:
            results.append(ok)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(results) == [False] * 7 + [True]
    assert count(fresh_stack, "raw.records") == 50


@pytest.mark.integration
def test_a_failure_midway_leaves_no_manifest_row_and_no_records(fresh_stack: Stack) -> None:
    class Unserializable:
        pass

    with pytest.raises(TypeError):
        load(fresh_stack, "half", [{"ok": 1}, {"bad": Unserializable()}])
    assert count(fresh_stack, "ops.file_manifest") == 0
    assert count(fresh_stack, "raw.records") == 0
    # and the same file can be loaded properly afterwards
    assert load(fresh_stack, "half", [{"ok": 1}]) is True


@pytest.mark.integration
def test_quarantine_row_and_latest_loaded_date(fresh_stack: Stack) -> None:
    wh = fresh_stack.warehouse
    assert (
        wh.record_quarantined(
            feed="vendor_c",
            path="p",
            checksum="q1",
            file_date=DAY,
            reason_code="contract_violation",
            reason="bad",
            size_bytes=5,
            run_id="t",
        )
        is True
    )
    assert (
        wh.record_quarantined(
            feed="vendor_c",
            path="p",
            checksum="q1",
            file_date=DAY,
            reason_code="x",
            reason="again",
            size_bytes=5,
            run_id="t",
        )
        is False
    )
    assert wh.status_of("q1") == "quarantined"
    assert wh.latest_loaded_date("vendor_c", date(2026, 3, 31)) is None  # quarantined is not loaded
    load(fresh_stack, "l1", [{"a": 1}], feed="vendor_c")
    assert wh.latest_loaded_date("vendor_c", date(2026, 3, 31)) == DAY
    assert wh.latest_loaded_date("vendor_c", date(2026, 2, 1)) is None


@pytest.mark.integration
def test_s3_round_trip_copy_list_delete(fresh_stack: Stack) -> None:
    store: S3Store = fresh_stack.store
    store.ensure_bucket()  # second call must be a no-op
    store.put("incoming/a/1.json", b"one")
    store.put("incoming/a/2.json", b"two")
    store.copy("incoming/a/1.json", "quarantine/a/1.json")
    assert store.get("quarantine/a/1.json") == b"one"
    assert store.list_keys("incoming/a/") == ["incoming/a/1.json", "incoming/a/2.json"]
    assert store.delete_prefix("incoming/") == 2
    assert store.list_keys("incoming/") == []
    assert store.list_keys("quarantine/") == ["quarantine/a/1.json"]


@pytest.mark.integration
def test_concurrent_first_start_does_not_collide_on_schema_creation(base_settings) -> None:  # type: ignore[no-untyped-def]
    """Eight workers start on an empty database at the same moment, as the mapped ingest tasks do.

    Without the advisory lock this raised UniqueViolation in most attempts.
    """
    from pipeline.warehouse import Warehouse
    from tests.services import create_database, drop_database

    errors: list[str] = []
    for trial in range(4):
        name = f"race_{trial}_{threading.get_ident()}"
        dsn = create_database(base_settings.warehouse_dsn, name)

        def worker(dsn: str = dsn) -> None:
            try:
                Warehouse(dsn).ensure_schema()
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{type(exc).__name__}: {exc}")

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        drop_database(base_settings.warehouse_dsn, name)
    assert errors == []
