from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest
from psycopg import conninfo

from tests.services import LoadedStack

ROOT = Path(__file__).resolve().parents[2]
DBT = Path(sys.executable).parent / "dbt"
DBT_TIMEOUT_SECONDS = 300


@dataclass(frozen=True)
class DbtResult:
    returncode: int
    output: str
    results: dict[str, str]  # unique_id -> status


@dataclass(frozen=True)
class DbtRunner:
    env: dict[str, str]
    target: Path

    def _db_activity(self) -> str:
        """What the database was doing when dbt stalled: sessions and who waits on whom."""
        import psycopg

        try:
            with psycopg.connect(
                host=self.env["DBT_HOST"],
                port=self.env["DBT_PORT"],
                user=self.env["DBT_USER"],
                password=self.env["DBT_PASSWORD"],
                dbname=self.env["DBT_DBNAME"],
                connect_timeout=5,
            ) as conn:
                sessions = conn.execute(
                    "select pid, state, wait_event_type, wait_event, left(query, 90) "
                    "from pg_stat_activity where datname = current_database()"
                ).fetchall()
                locks = conn.execute(
                    "select pid, locktype, mode, granted from pg_locks where not granted"
                ).fetchall()
            return f"sessions: {sessions}\nungranted locks: {locks}"
        except Exception as exc:  # noqa: BLE001
            return f"could not read database activity: {exc}"

    def __call__(self, *args: str) -> DbtResult:
        try:
            proc = subprocess.run(
                [
                    str(DBT),
                    *args,
                    "--profiles-dir",
                    str(ROOT / "dbt"),
                    "--project-dir",
                    str(ROOT / "dbt"),
                    "--target-path",
                    str(self.target),
                    "--log-path",
                    str(self.target / "logs"),
                ],
                capture_output=True,
                text=True,
                env=self.env,
                cwd=ROOT,
                timeout=DBT_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as exc:
            pytest.fail(
                f"dbt {' '.join(args)} timed out after {DBT_TIMEOUT_SECONDS}s.\n"
                f"stdout tail: {(exc.stdout or b'')[-1500:]!r}\n{self._db_activity()}"
            )
        results: dict[str, str] = {}
        run_results = self.target / "run_results.json"
        if run_results.exists():
            for r in json.loads(run_results.read_text())["results"]:
                results[r["unique_id"]] = r["status"]
        return DbtResult(proc.returncode, proc.stdout + proc.stderr, results)


@pytest.fixture(scope="session")
def dbt(loaded_stack: LoadedStack, tmp_path_factory: pytest.TempPathFactory) -> DbtRunner:
    parts = conninfo.conninfo_to_dict(loaded_stack.stack.settings.warehouse_dsn)
    env = {
        **os.environ,
        "DBT_HOST": str(parts["host"]),
        "DBT_PORT": str(parts["port"]),
        "DBT_USER": str(parts["user"]),
        "DBT_PASSWORD": str(parts["password"]),
        "DBT_DBNAME": str(parts["dbname"]),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    return DbtRunner(env, tmp_path_factory.mktemp("dbt-target"))


@pytest.fixture(scope="session")
def built(dbt: DbtRunner) -> DbtResult:
    result = dbt("build")
    assert result.returncode == 0, result.output[-3000:]
    return result


@pytest.fixture
def query(loaded_stack: LoadedStack) -> Callable[..., list[dict[str, object]]]:
    return loaded_stack.stack.warehouse.fetch_all
