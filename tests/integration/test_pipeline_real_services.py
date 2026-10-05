from __future__ import annotations

import json
from datetime import date

import pytest

from pipeline import cli
from pipeline.feeds import FEEDS, incoming_key, quarantine_key, reason_key
from pipeline.freshness import check_freshness
from pipeline.ingest import ingest_feed
from pipeline.report import collect, render_html
from tests.services import LoadedStack, Stack, upload_dataset

DRIFT_KEY = "incoming/vendor_c/2026-03-05.json"


def scalar(stack: Stack, sql: str) -> int:
    return int(stack.warehouse.fetch_all(sql)[0]["n"])


@pytest.mark.integration
def test_full_dataset_loads_everything_except_the_drift_file(
    loaded_stack: LoadedStack, facts: dict[str, object]
) -> None:
    stack = loaded_stack.stack
    status = {
        (r["feed"], r["status"]): int(r["n"])
        for r in stack.warehouse.fetch_all(
            "select feed, status, count(*) as n from ops.file_manifest group by 1, 2"
        )
    }
    assert status[("vendor_a", "loaded")] == 30
    assert status[("vendor_b", "loaded")] == 30
    assert status[("orders", "loaded")] == 30
    assert status[("vendor_c", "loaded")] == 29
    assert status[("vendor_c", "quarantined")] == 1
    assert ("vendor_a", "quarantined") not in status
    assert (
        scalar(stack, "select count(*) as n from raw.records where feed = 'orders'")
        == facts["orders"]
    )
    vendor_rows = {
        r["feed"]: int(r["n"])
        for r in stack.warehouse.fetch_all(
            "select feed, count(*) as n from raw.records where feed like 'vendor%' group by 1"
        )
    }
    per_vendor = facts["vendor_products"]
    assert isinstance(per_vendor, dict)
    assert vendor_rows["vendor_a"] == per_vendor["vendor_a"] * 30
    assert vendor_rows["vendor_c"] == per_vendor["vendor_c"] * 29  # no row from the drift day


@pytest.mark.integration
def test_the_drift_file_is_in_quarantine_with_a_reason_and_other_vendors_loaded(
    loaded_stack: LoadedStack,
) -> None:
    stack = loaded_stack.stack
    assert quarantine_key(DRIFT_KEY) in stack.store.list_keys("quarantine/")
    reason = json.loads(stack.store.get(reason_key(DRIFT_KEY)))
    assert reason["code"] == "contract_violation"
    assert "missing required field 'Cost'" in reason["reason"]
    assert "UnitCost" in reason["reason"]
    same_day = {
        r["feed"]
        for r in stack.warehouse.fetch_all(
            "select feed from ops.file_manifest "
            "where file_date = '2026-03-05' and status = 'loaded'"
        )
    }
    assert same_day == {"vendor_a", "vendor_b", "orders"}
    assert [len(loaded_stack.results[f].quarantined) for f in FEEDS] == [0, 0, 1, 0]


@pytest.mark.integration
def test_rerunning_the_same_files_loads_zero_new_rows(loaded_stack: LoadedStack) -> None:
    stack = loaded_stack.stack
    before = scalar(stack, "select count(*) as n from raw.records")
    manifest_before = scalar(stack, "select count(*) as n from ops.file_manifest")
    for feed in FEEDS:
        again = ingest_feed(stack.store, stack.warehouse, feed, run_id="second")
        assert again.loaded == [] and again.quarantined == []
        assert len(again.skipped) == 30  # vendor_c: 29 loaded files plus the quarantined one
    assert scalar(stack, "select count(*) as n from raw.records") == before
    assert scalar(stack, "select count(*) as n from ops.file_manifest") == manifest_before


@pytest.mark.integration
def test_backfill_by_date_window_loads_only_that_window(fresh_stack: Stack, dataset) -> None:  # type: ignore[no-untyped-def]
    upload_dataset(fresh_stack.store, dataset)
    result = ingest_feed(
        fresh_stack.store,
        fresh_stack.warehouse,
        "orders",
        start=date(2026, 3, 10),
        end=date(2026, 3, 12),
        run_id="backfill",
    )
    assert [o.key for o in result.loaded] == [
        incoming_key("orders", date(2026, 3, d)) for d in (10, 11, 12)
    ]
    assert scalar(fresh_stack, "select count(distinct file_date) as n from ops.file_manifest") == 3


@pytest.mark.integration
def test_a_corrected_resend_replaces_the_quarantined_day(fresh_stack: Stack, dataset) -> None:  # type: ignore[no-untyped-def]
    upload_dataset(fresh_stack.store, dataset)
    first = ingest_feed(fresh_stack.store, fresh_stack.warehouse, "vendor_c", run_id="r1")
    assert len(first.quarantined) == 1
    fixed = fresh_stack.store.get(DRIFT_KEY).replace(b'"UnitCost"', b'"Cost"')
    fresh_stack.store.put(DRIFT_KEY, fixed)
    second = ingest_feed(fresh_stack.store, fresh_stack.warehouse, "vendor_c", run_id="r2")
    assert [o.status for o in second.outcomes if o.status != "skipped"] == ["loaded"]
    assert (
        scalar(
            fresh_stack,
            "select count(*) as n from ops.file_manifest where feed='vendor_c' and status='loaded'",
        )
        == 30
    )


@pytest.mark.integration
def test_freshness_against_the_real_manifest(loaded_stack: LoadedStack) -> None:
    results = {
        r.feed: r
        for r in check_freshness(loaded_stack.stack.warehouse, FEEDS, as_of=date(2026, 3, 5))
    }
    assert not any(r.stale for r in results.values())
    assert results["vendor_c"].lag_days == 1  # day 5 was quarantined, day 4 is the latest loaded
    late = check_freshness(loaded_stack.stack.warehouse, FEEDS, as_of=date(2026, 4, 15))
    assert all(r.stale for r in late)


@pytest.mark.integration
def test_report_before_dbt_says_so_and_lists_the_rejected_file(loaded_stack: LoadedStack) -> None:
    data = collect(loaded_stack.stack.warehouse)
    assert data.sku_match is None and data.match_rate_pct is None
    assert all(n is None for n in data.mart_counts.values())
    page = render_html(data)
    assert "not built yet" in page and "Run dbt to build the crosswalk" in page
    assert DRIFT_KEY in page and "UnitCost" in page
    by_feed = {f.feed: f for f in data.feeds}
    assert by_feed["vendor_c"].files_quarantined == 1 and by_feed["vendor_c"].status == "fresh"


@pytest.mark.integration
def test_cli_end_to_end_with_real_services(
    fresh_stack: Stack,
    dataset,
    tmp_path,
    monkeypatch,
    capsys,  # type: ignore[no-untyped-def]
) -> None:
    s = fresh_stack.settings
    monkeypatch.setenv("WAREHOUSE_DSN", s.warehouse_dsn)
    monkeypatch.setenv("S3_BUCKET", s.s3_bucket)
    assert cli.main(["init-db"]) == 0
    assert cli.main(["upload", "--source", str(dataset)]) == 0
    assert cli.main(["ingest", "--feed", "vendor_c", "--end", "2026-03-06"]) == 0
    assert (
        cli.main(["ingest", "--feed", "vendor_c", "--end", "2026-03-06", "--fail-on-quarantine"])
        == 0
    )  # already seen
    out = tmp_path / "r.html"
    assert (
        cli.main(["report", "--out", str(out)]) == 0 and "Pipeline health report" in out.read_text()
    )
    assert cli.main(["freshness", "--as-of", "2026-03-06"]) == 1  # other feeds were never loaded
    capsys.readouterr()
    assert cli.main(["reset"]) == 0
    assert "quarantine objects removed" in capsys.readouterr().out
    assert fresh_stack.store.list_keys("quarantine/") == []
