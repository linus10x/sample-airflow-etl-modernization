"""DAG integrity checks. They need Airflow, so they run in the Airflow image (`make test-dags`)."""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

pytestmark = pytest.mark.dags

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"
EXPECTED = {"ingest_vendor_files", "transform"}


@pytest.fixture(scope="module")
def dagbag():  # type: ignore[no-untyped-def]
    from airflow.dag_processing.dagbag import DagBag

    return DagBag(dag_folder=str(DAGS_DIR), bundle_name="dags-folder", bundle_path=DAGS_DIR)


def test_every_dag_file_imports_cleanly(dagbag) -> None:  # type: ignore[no-untyped-def]
    assert dagbag.import_errors == {}


def test_both_dags_are_present_and_each_is_defined_once(dagbag) -> None:  # type: ignore[no-untyped-def]
    assert set(dagbag.dags) == EXPECTED


def test_no_dag_has_a_cycle(dagbag) -> None:  # type: ignore[no-untyped-def]
    for dag in dagbag.dags.values():
        state: dict[str, int] = {}

        def visit(task_id: str, dag=dag, state=state) -> None:  # type: ignore[no-untyped-def]
            if state.get(task_id) == 1:
                raise AssertionError(f"cycle through {task_id} in {dag.dag_id}")
            if state.get(task_id) == 2:
                return
            state[task_id] = 1
            for child in dag.task_dict[task_id].downstream_task_ids:
                visit(child)
            state[task_id] = 2

        for task_id in dag.task_dict:
            visit(task_id)


def test_every_dag_has_an_owner_other_than_the_default_and_tags(dagbag) -> None:  # type: ignore[no-untyped-def]
    for dag in dagbag.dags.values():
        assert dag.default_args.get("owner") not in (None, "", "airflow"), dag.dag_id
        assert dag.tags, dag.dag_id
        assert all(task.owner == dag.default_args["owner"] for task in dag.tasks)


def test_every_dag_sets_retries_and_ingest_uses_exponential_backoff(dagbag) -> None:  # type: ignore[no-untyped-def]
    for dag in dagbag.dags.values():
        assert int(dag.default_args["retries"]) >= 1, dag.dag_id
    ingest = dagbag.dags["ingest_vendor_files"]
    assert ingest.default_args["retry_exponential_backoff"] is True
    assert ingest.task_dict["ingest"].retries >= 1


def test_ingest_maps_one_task_per_feed_and_the_transform_dag_waits_on_the_asset(dagbag) -> None:  # type: ignore[no-untyped-def]
    from dags._shared import RAW_RECORDS

    ingest = dagbag.dags["ingest_vendor_files"]
    assert ingest.task_dict["ingest"].is_mapped
    outlet_names = {o.name for o in ingest.task_dict["publish_raw_records"].outlets}
    assert outlet_names == {RAW_RECORDS.name}
    transform = dagbag.dags["transform"]
    assert not isinstance(transform.timetable.__class__.__name__, type(None))
    assert (
        "Asset" in transform.timetable.__class__.__name__
        or "Dataset" in transform.timetable.__class__.__name__
    )


def test_dag_files_do_no_heavy_work_at_import_time() -> None:
    """Parsing a DAG file must not open connections or import the database and S3 clients."""
    probe = textwrap.dedent(
        f"""
        import importlib, json, sys, time
        sys.path.insert(0, {str(DAGS_DIR.parent)!r})
        t = time.perf_counter()
        for name in ("dags.ingest_vendor_files", "dags.transform"):
            importlib.import_module(name)
        print(json.dumps({{
            "seconds": time.perf_counter() - t,
            "heavy": sorted(m for m in ("psycopg", "pipeline.warehouse", "pipeline.storage")
                            if m in sys.modules),
        }}))
        """
    )
    out = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=True)
    result = __import__("json").loads(out.stdout.strip().splitlines()[-1])
    assert result["heavy"] == []
    assert result["seconds"] < 10
