"""Build the dbt models and render the health report. Runs when the raw records asset updates."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from dags._shared import RAW_RECORDS

DBT = "/home/airflow/dbt-venv/bin/dbt"
PROJECT = "/opt/airflow/project/dbt"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
}


@dag(
    dag_id="transform",
    schedule=[RAW_RECORDS],
    start_date=pendulum.datetime(2026, 3, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["etl", "dbt"],
    doc_md=__doc__,
)
def transform() -> None:
    # BashOperator rather than the Cosmos provider: one dbt invocation, readable logs, and no
    # extra dependency to pin against Airflow.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {PROJECT} && {DBT} build --profiles-dir . "
            "--target-path /tmp/dbt-target --log-path /tmp/dbt-logs"
        ),
    )

    @task
    def render_report() -> str:
        from pathlib import Path

        from pipeline.config import Settings
        from pipeline.report import collect, render_html
        from pipeline.warehouse import Warehouse

        out = Path("/opt/airflow/project/out/health_report.html")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            render_html(collect(Warehouse(Settings.from_env().warehouse_dsn))), encoding="utf-8"
        )
        return str(out)

    dbt_build >> render_report()


transform()
