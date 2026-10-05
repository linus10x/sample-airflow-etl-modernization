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


@dataclass(frozen=True)
class DbtResult:
    returncode: int
    output: str
    results: dict[str, str]  # unique_id -> status


@dataclass(frozen=True)
class DbtRunner:
    env: dict[str, str]
    target: Path

    def __call__(self, *args: str) -> DbtResult:
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
