"""Ingest vendor and order JSON from S3 into Postgres.

One mapped task per feed. A bad file is quarantined and its feed's task goes red, so the failure is
loud, while the other feeds load on their own. The final task publishes the raw-records asset, and
the `transform` DAG is scheduled on that asset. Heavy imports stay inside the task bodies so the
scheduler can parse this file cheaply.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pendulum
from airflow.sdk import Param, TriggerRule, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from dags._shared import RAW_RECORDS
from pipeline.feeds import FEEDS

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(seconds=20),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
}


def file_date(context: Any) -> date:
    """The day this run covers. Manual runs without a logical date fall back to the run time."""
    moment = context.get("logical_date") or context["dag_run"].run_after
    return moment.date()  # type: ignore[no-any-return]


@dag(
    dag_id="ingest_vendor_files",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 3, 1, tz="UTC"),
    # The sample's synthetic files stop on 2026-03-30. Without an end date, the scheduler would
    # also run for today, find no file, and fail the freshness check. Remove this for live data.
    end_date=pendulum.datetime(2026, 3, 30, tz="UTC"),
    catchup=False,
    max_active_runs=4,
    default_args=DEFAULT_ARGS,
    params={
        "lookback_days": Param(
            0,
            type="integer",
            minimum=0,
            description="Also pick up files from this many earlier days",
        )
    },
    tags=["etl", "ingest"],
    doc_md=__doc__,
)
def ingest_vendor_files() -> None:
    @task(map_index_template="{{ feed_label }}")
    def ingest(feed: str) -> dict[str, Any]:
        from pipeline.config import Settings
        from pipeline.ingest import ingest_feed
        from pipeline.notify import get_notifier
        from pipeline.storage import S3Store
        from pipeline.warehouse import Warehouse

        context = get_current_context()
        settings = Settings.from_env()
        end = file_date(context)
        start = end - timedelta(days=int(context["params"]["lookback_days"]))
        warehouse = Warehouse(settings.warehouse_dsn)
        warehouse.ensure_schema()
        result = ingest_feed(
            S3Store.from_settings(settings),
            warehouse,
            feed,
            start=start,
            end=end,
            run_id=context["run_id"],
            notifier=get_notifier(settings),
        )
        context["feed_label"] = (
            f"{feed}: {len(result.loaded)} loaded, {len(result.quarantined)} quarantined, "
            f"{len(result.skipped)} already loaded"
        )
        print(result.summary())
        if result.quarantined:
            reasons = "; ".join(f"{o.key}: {o.reason}" for o in result.quarantined)
            # A contract failure will not fix itself, so fail without retrying. Good files from
            # this feed were already committed above.
            raise AirflowFailException(f"{len(result.quarantined)} file(s) quarantined. {reasons}")
        return {"feed": feed, "loaded": len(result.loaded), "rows": result.rows_loaded}

    @task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def check_freshness() -> None:
        from pipeline.config import Settings
        from pipeline.freshness import check_freshness as run_check
        from pipeline.notify import get_notifier
        from pipeline.warehouse import Warehouse

        settings = Settings.from_env()
        results = run_check(
            Warehouse(settings.warehouse_dsn), FEEDS, file_date(get_current_context())
        )
        for r in results:
            print(r.describe())
        stale = [r for r in results if r.stale]
        if stale:
            message = "; ".join(r.describe() for r in stale)
            get_notifier(settings).send("Feed freshness check failed", message)
            raise AirflowFailException(message)

    @task(outlets=[RAW_RECORDS], trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def publish_raw_records() -> None:
        """Marks the raw tables as updated so the transform DAG runs. Idempotent by design."""

    ingested = ingest.expand(feed=list(FEEDS))
    ingested >> [check_freshness(), publish_raw_records()]


ingest_vendor_files()
