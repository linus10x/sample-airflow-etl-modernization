"""Drive the real DAGs in the compose Airflow container. Needs `make up`.

This resets the demo warehouse and bucket, so run it on a stack you are happy to wipe.
The DAGs are paused first: `airflow dags test` creates an ordinary run, and an unpaused DAG
would let the scheduler execute the same run a second time in parallel.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from pipeline.config import Settings
from pipeline.storage import S3Store
from pipeline.warehouse import Warehouse
from tests.e2e.conftest import ROOT
from tests.services import upload_dataset

pytestmark = pytest.mark.e2e

DAYS = ("2026-03-04", "2026-03-05", "2026-03-06")


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", *args], cwd=ROOT, capture_output=True, text=True, check=check
    )


def airflow(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return compose("exec", "-T", "airflow", "airflow", *args, check=check)


def airflow_db(sql: str) -> list[list[str]]:
    out = compose(
        "exec",
        "-T",
        "postgres",
        "psql",
        "-U",
        "warehouse",
        "-d",
        "airflow",
        "-At",
        "-F",
        "|",
        "-c",
        sql,
    ).stdout
    return [line.split("|") for line in out.strip().splitlines() if line]


@pytest.fixture(scope="module")
def after_three_days(dataset: Path):  # type: ignore[no-untyped-def]
    settings = Settings.from_env()
    store = S3Store.from_settings(settings)
    store.ensure_bucket()
    Warehouse(settings.warehouse_dsn).reset()
    store.delete_prefix("")
    upload_dataset(store, dataset)
    airflow("dags", "pause", "ingest_vendor_files", check=False)
    airflow("dags", "pause", "transform", check=False)
    outputs = {d: airflow("dags", "test", "ingest_vendor_files", d, check=False) for d in DAYS}
    outputs["transform"] = airflow("dags", "test", "transform", DAYS[-1], check=False)
    return Warehouse(settings.warehouse_dsn), store, outputs


def task_states(logical_date: str, dag_id: str = "ingest_vendor_files") -> dict[str, str]:
    rows = airflow_db(
        "select ti.task_id || '#' || ti.map_index, ti.state from task_instance ti "
        "join dag_run dr on dr.dag_id = ti.dag_id and dr.run_id = ti.run_id "
        f"where dr.dag_id = '{dag_id}' and dr.logical_date::date = '{logical_date}' "
        f"and dr.id = (select max(id) from dag_run where dag_id = '{dag_id}' "
        f"and logical_date::date = '{logical_date}')"
    )
    return {r[0]: r[1] for r in rows}


def test_the_drift_day_fails_only_the_vendor_c_task(after_three_days) -> None:  # type: ignore[no-untyped-def]
    # map_index order follows pipeline.feeds.FEEDS: vendor_a, vendor_b, vendor_c, orders
    states = task_states("2026-03-05")
    assert states["ingest#0"] == "success"
    assert states["ingest#1"] == "success"
    assert states["ingest#2"] == "failed"
    assert states["ingest#3"] == "success"
    assert states["check_freshness#-1"] == "success"
    assert states["publish_raw_records#-1"] == "success"


def test_clean_days_have_no_failed_task(after_three_days) -> None:  # type: ignore[no-untyped-def]
    for day in ("2026-03-04", "2026-03-06"):
        assert "failed" not in task_states(day).values(), day


def test_the_failure_is_not_retried(after_three_days) -> None:  # type: ignore[no-untyped-def]
    tries = airflow_db(
        "select max(ti.try_number) from task_instance ti join dag_run dr "
        "on dr.dag_id = ti.dag_id and dr.run_id = ti.run_id "
        "where dr.dag_id = 'ingest_vendor_files' and dr.logical_date::date = '2026-03-05' "
        "and ti.task_id = 'ingest' and ti.map_index = 2"
    )
    assert tries == [["1"]]


def test_manifest_shows_one_quarantine_and_everything_else_loaded(after_three_days) -> None:  # type: ignore[no-untyped-def]
    wh, store, _ = after_three_days
    rows = wh.fetch_all(
        "select feed, file_date::text as d, status from ops.file_manifest order by 1, 2"
    )
    quarantined = [(r["feed"], r["d"]) for r in rows if r["status"] == "quarantined"]
    assert quarantined == [("vendor_c", "2026-03-05")]
    assert sum(1 for r in rows if r["status"] == "loaded") == 11
    assert "quarantine/vendor_c/2026-03-05.json.reason.json" in store.list_keys("quarantine/")


def test_transform_dag_builds_marts_and_writes_the_report(after_three_days, dataset: Path) -> None:  # type: ignore[no-untyped-def]
    wh, _, outputs = after_three_days
    assert "DagRun Finished" in outputs["transform"].stdout + outputs["transform"].stderr
    expected_orders = sum(
        len(json.loads((dataset / "incoming" / "orders" / f"{d}.json").read_text())["orders"])
        for d in DAYS
    )
    assert (
        int(wh.fetch_all("select count(*) as n from marts.fct_orders")[0]["n"]) == expected_orders
    )
    report = (ROOT / "out" / "health_report.html").read_text()
    assert "incoming/vendor_c/2026-03-05.json" in report and "UnitCost" in report
    assert states_of_transform() == "success"


def states_of_transform() -> str:
    rows = airflow_db(
        "select state from dag_run where dag_id = 'transform' order by id desc limit 1"
    )
    return rows[0][0]
