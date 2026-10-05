from __future__ import annotations

import csv
import subprocess
import sys
from collections.abc import Callable, Iterator
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest

from pipeline.report import collect, render_html
from tests.e2e.conftest import ROOT, DbtResult, DbtRunner
from tests.services import LoadedStack

pytestmark = pytest.mark.e2e

Query = Callable[..., list[dict[str, object]]]


def n(query: Query, sql: str) -> int:
    return int(query(sql)[0]["n"])  # type: ignore[call-overload]


def run_legacy(dataset: Path, out: Path) -> None:
    subprocess.run(
        [sys.executable, str(ROOT / "legacy" / "legacy_etl.py"), str(dataset), str(out)],
        check=True,
        capture_output=True,
    )


def test_dbt_build_passes_with_tests_and_unit_tests(built: DbtResult) -> None:
    assert set(built.results.values()) <= {"success", "pass"}, built.output[-2000:]
    ids = list(built.results)
    assert any(
        i.startswith("unit_test.") and "normalize_sku_handles_each_vendor_format" in i for i in ids
    )
    assert any("no_dropped_lines" in i for i in ids)
    assert any("unmatched_lines_are_labelled" in i for i in ids)
    assert any("assert_macro_matches_shared_cases" in i for i in ids)


def test_marts_row_counts_match_the_generated_dataset(
    built: DbtResult, query: Query, facts: dict[str, object]
) -> None:
    assert n(query, "select count(*) as n from marts.dim_products") == facts["products"]
    assert n(query, "select count(*) as n from marts.fct_orders") == facts["orders"]
    assert n(query, "select count(*) as n from marts.fct_order_lines") == facts["order_lines"]


def test_quarantined_day_never_reached_the_marts(built: DbtResult, query: Query) -> None:
    c = query(
        "select distinct snapshot_count as c from marts.dim_products where vendor = 'vendor_c'"
    )
    assert [r["c"] for r in c] == [29]
    o = query(
        "select distinct snapshot_count as c from marts.dim_products where vendor <> 'vendor_c'"
    )
    assert [r["c"] for r in o] == [30]


def test_crosswalk_uses_all_three_outcomes_and_keeps_every_line(
    built: DbtResult, query: Query
) -> None:
    rows = query("select match_method, line_count from marts.sku_match_summary")
    summary = {r["match_method"]: int(r["line_count"]) for r in rows}  # type: ignore[call-overload]
    assert set(summary) == {"exact_normalized", "manual_override", "unmatched"}
    assert all(v > 0 for v in summary.values())
    assert sum(summary.values()) == n(query, "select count(*) as n from marts.fct_order_lines")


def test_new_order_totals_match_the_legacy_script_for_every_order(
    built: DbtResult, query: Query, dataset: Path, tmp_path: Path
) -> None:
    run_legacy(dataset, tmp_path)
    with (tmp_path / "orders.csv").open() as fh:
        legacy = {
            r["order_id"]: (int(r["line_count"]), Decimal(r["order_total"]))
            for r in csv.DictReader(fh)
        }
    rows = query("select order_id, line_count, order_total from marts.fct_orders")
    new = {r["order_id"]: (int(r["line_count"]), Decimal(r["order_total"])) for r in rows}  # type: ignore[call-overload]
    assert len(legacy) == len(new) == 5000
    assert legacy.keys() == new.keys()
    assert [k for k in legacy if legacy[k] != new[k]] == []


def test_legacy_silently_wrote_the_bad_day_that_the_new_pipeline_refused(
    built: DbtResult, query: Query, dataset: Path, tmp_path: Path
) -> None:
    run_legacy(dataset, tmp_path)
    with (tmp_path / "vendor_products.csv").open() as fh:
        bad = [
            r
            for r in csv.DictReader(fh)
            if r["vendor"] == "vendor_c" and r["snapshot_date"] == "2026-03-05"
        ]
    assert len(bad) == 240 and {r["cost"] for r in bad} == {"0"}  # cost became 0, no error
    assert n(query, "select count(*) as n from raw.records where payload ? 'UnitCost'") == 0


def test_health_report_after_dbt_shows_the_match_rate(
    built: DbtResult, loaded_stack: LoadedStack
) -> None:
    data = collect(loaded_stack.stack.warehouse)
    assert data.match_rate_pct is not None and 0 < data.match_rate_pct < 100
    assert all(isinstance(c, int) for c in data.mart_counts.values())
    page = render_html(data)
    assert f"{data.match_rate_pct}%" in page and "not built yet" not in page


def test_dbt_build_is_repeatable(built: DbtResult, dbt: DbtRunner, query: Query) -> None:
    before = n(query, "select count(*) as n from marts.fct_order_lines")
    assert dbt("build").returncode == 0
    assert n(query, "select count(*) as n from marts.fct_order_lines") == before


# Negative controls: damage the data on purpose and confirm the dbt gate goes red. A gate that
# cannot fail proves nothing. Each control rebuilds the model afterwards.


@pytest.fixture
def damage(
    loaded_stack: LoadedStack, dbt: DbtRunner, built: DbtResult
) -> Iterator[Callable[[str], None]]:
    def apply(sql: str) -> None:
        with psycopg.connect(loaded_stack.stack.settings.warehouse_dsn, autocommit=True) as conn:
            conn.execute(sql)  # type: ignore[call-overload]

    yield apply
    assert dbt("run", "--select", "fct_order_lines+").returncode == 0  # restore


def failed_ids(result: DbtResult) -> list[str]:
    return [k for k, v in result.results.items() if v in {"fail", "error"}]


def test_negative_control_deleting_order_lines_trips_no_dropped_lines(
    damage: Callable[[str], None], dbt: DbtRunner
) -> None:
    damage(
        "delete from marts.fct_order_lines where (order_id, line_no) in "
        "(select order_id, line_no from marts.fct_order_lines order by order_id, line_no limit 7)"
    )
    result = dbt("test", "--select", "fct_order_lines")
    assert result.returncode != 0
    assert any("no_dropped_lines" in i for i in failed_ids(result))


def test_negative_control_unlabelled_unmatched_line_trips_the_label_check(
    damage: Callable[[str], None], dbt: DbtRunner
) -> None:
    damage(
        "update marts.fct_order_lines set product_key = null, match_method = 'exact_normalized' "
        "where (order_id, line_no) in (select order_id, line_no from marts.fct_order_lines "
        "where match_method = 'exact_normalized' limit 3)"
    )
    result = dbt("test", "--select", "fct_order_lines")
    assert result.returncode != 0
    assert any("unmatched_lines_are_labelled" in i for i in failed_ids(result))


def test_negative_control_pointer_to_a_missing_product_trips_relationships(
    damage: Callable[[str], None], dbt: DbtRunner
) -> None:
    damage(
        "update marts.fct_order_lines set product_key = 'vendor_a:does-not-exist' "
        "where (order_id, line_no) in (select order_id, line_no from marts.fct_order_lines "
        "where match_method = 'exact_normalized' limit 2)"
    )
    result = dbt("test", "--select", "fct_order_lines")
    assert any("relationships_fct_order_lines_product_key" in i for i in failed_ids(result))
