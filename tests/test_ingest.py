from __future__ import annotations

from datetime import date
from typing import Any

from pipeline.feeds import incoming_key, quarantine_key, reason_key
from pipeline.ingest import discover, ingest_feed
from pipeline.storage import MemoryStore
from tests.conftest import (
    FakeWarehouse,
    RecordingNotifier,
    as_bytes,
    orders_payload,
    vendor_a_payload,
    vendor_c_payload,
)


def put(store: MemoryStore, feed: str, day: str, payload: Any) -> str:
    key = incoming_key(feed, date.fromisoformat(day))
    store.put(key, payload if isinstance(payload, bytes) else as_bytes(payload))
    return key


def test_valid_files_are_loaded_and_counted(store: MemoryStore, warehouse: FakeWarehouse) -> None:
    put(store, "vendor_a", "2026-03-01", vendor_a_payload("2026-03-01", n=3))
    put(store, "vendor_a", "2026-03-02", vendor_a_payload("2026-03-02", n=4))
    result = ingest_feed(store, warehouse, "vendor_a")
    assert [o.status for o in result.outcomes] == ["loaded", "loaded"]
    assert result.rows_loaded == 7
    assert "2 loaded (7 rows)" in result.summary()


def test_bad_vendor_file_is_quarantined_with_a_readable_reason(
    store: MemoryStore, warehouse: FakeWarehouse, notifier: RecordingNotifier
) -> None:
    put(store, "vendor_c", "2026-03-04", vendor_c_payload("2026-03-04"))
    bad = put(
        store, "vendor_c", "2026-03-05", vendor_c_payload("2026-03-05", cost_field="UnitCost")
    )
    put(store, "vendor_c", "2026-03-06", vendor_c_payload("2026-03-06"))

    result = ingest_feed(store, warehouse, "vendor_c", notifier=notifier)

    assert [o.status for o in result.outcomes] == [
        "loaded",
        "quarantined",
        "loaded",
    ]  # others unaffected
    assert quarantine_key(bad) in store.objects
    assert b"missing required field 'Cost'" in store.objects[reason_key(bad)]
    quarantined = [m for m in warehouse.manifest.values() if m["status"] == "quarantined"]
    assert len(quarantined) == 1 and "UnitCost" in quarantined[0]["reason"]
    assert len(notifier.sent) == 1 and bad in notifier.sent[0][0]


def test_rerunning_the_same_files_loads_nothing_new(
    store: MemoryStore, warehouse: FakeWarehouse, notifier: RecordingNotifier
) -> None:
    put(store, "vendor_c", "2026-03-04", vendor_c_payload("2026-03-04"))
    put(store, "vendor_c", "2026-03-05", vendor_c_payload("2026-03-05", cost_field="UnitCost"))
    first = ingest_feed(store, warehouse, "vendor_c", notifier=notifier)
    second = ingest_feed(store, warehouse, "vendor_c", notifier=notifier)
    assert len(first.loaded) == 1 and len(first.quarantined) == 1
    assert second.loaded == [] and second.quarantined == []
    assert len(second.skipped) == 2
    assert len(notifier.sent) == 1  # no second alert for a file already quarantined
    assert len(warehouse.records) == 1


def test_identical_bytes_at_another_path_are_not_loaded_twice(
    store: MemoryStore, warehouse: FakeWarehouse
) -> None:
    payload = vendor_a_payload("2026-03-01")
    put(store, "vendor_a", "2026-03-01", payload)
    ingest_feed(store, warehouse, "vendor_a")
    store.put("incoming/vendor_a/2026-03-01.json", as_bytes(payload))  # same content again
    assert ingest_feed(store, warehouse, "vendor_a").loaded == []


def test_a_corrected_resend_of_a_quarantined_file_loads(
    store: MemoryStore, warehouse: FakeWarehouse
) -> None:
    key = put(
        store, "vendor_c", "2026-03-05", vendor_c_payload("2026-03-05", cost_field="UnitCost")
    )
    assert ingest_feed(store, warehouse, "vendor_c").quarantined
    store.put(key, as_bytes(vendor_c_payload("2026-03-05")))  # vendor fixes and re-sends
    result = ingest_feed(store, warehouse, "vendor_c")
    assert [o.status for o in result.outcomes] == ["loaded"]


def test_date_window_limits_what_is_discovered(store: MemoryStore) -> None:
    for d in ("2026-03-01", "2026-03-02", "2026-03-03"):
        put(store, "orders", d, orders_payload(d))
    found = discover(store, "orders", start=date(2026, 3, 2), end=date(2026, 3, 2))
    assert found == [("incoming/orders/2026-03-02.json", date(2026, 3, 2))]
    assert len(discover(store, "orders")) == 3


def test_backfill_by_date_loads_only_the_window(
    store: MemoryStore, warehouse: FakeWarehouse
) -> None:
    for d in ("2026-03-01", "2026-03-02", "2026-03-03"):
        put(store, "orders", d, orders_payload(d))
    result = ingest_feed(store, warehouse, "orders", start=date(2026, 3, 1), end=date(2026, 3, 2))
    assert len(result.loaded) == 2


def test_badly_named_file_is_quarantined_not_ignored(
    store: MemoryStore, warehouse: FakeWarehouse
) -> None:
    store.put("incoming/vendor_a/latest.json", as_bytes(vendor_a_payload()))
    result = ingest_feed(store, warehouse, "vendor_a")
    assert [o.status for o in result.outcomes] == ["quarantined"]
    assert "does not match" in (result.outcomes[0].reason or "")


def test_invalid_json_is_quarantined(store: MemoryStore, warehouse: FakeWarehouse) -> None:
    put(store, "vendor_a", "2026-03-01", b'{"vendor": "vendor_a", ')
    result = ingest_feed(store, warehouse, "vendor_a")
    assert result.quarantined and "not valid JSON" in (result.quarantined[0].reason or "")


def test_unknown_feed_raises(store: MemoryStore, warehouse: FakeWarehouse) -> None:
    try:
        ingest_feed(store, warehouse, "vendor_z")
    except ValueError as exc:
        assert "vendor_z" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected ValueError")


def test_on_file_callback_sees_every_outcome(store: MemoryStore, warehouse: FakeWarehouse) -> None:
    put(store, "vendor_a", "2026-03-01", vendor_a_payload("2026-03-01"))
    seen: list[str] = []
    ingest_feed(store, warehouse, "vendor_a", on_file=lambda o: seen.append(o.status))
    assert seen == ["loaded"]
