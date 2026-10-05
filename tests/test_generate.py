"""The generator and the contracts must agree: only the planted drift file may be rejected."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest

from pipeline.contracts import validate_file
from pipeline.errors import FileRejected
from pipeline.feeds import parse_incoming_key

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def generated(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out = tmp_path_factory.mktemp("gen")
    overrides = out / "overrides.csv"
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "generate.py"),
            "--seed",
            "42",
            "--out",
            str(out),
            "--overrides-out",
            str(overrides),
        ],
        check=True,
        capture_output=True,
    )
    return out


def test_dataset_has_the_documented_shape(generated: Path) -> None:
    facts = json.loads((generated / "FACTS.json").read_text())
    assert facts["products"] == 800
    assert facts["orders"] == 5000
    assert facts["days"] == 30
    assert sum(facts["vendor_products"].values()) == 800
    assert facts["files"] == len(list((generated / "incoming").rglob("*.json")))


def test_exactly_one_file_breaks_its_contract_and_it_is_the_day_five_vendor_c_file(
    generated: Path,
) -> None:
    rejected: list[str] = []
    for path in sorted((generated / "incoming").rglob("*.json")):
        key = path.relative_to(generated).as_posix()
        parsed = parse_incoming_key(key)
        assert parsed is not None
        feed, day = parsed
        try:
            validate_file(feed, json.loads(path.read_text()), day)
        except FileRejected as exc:
            rejected.append(f"{key}: {exc.reason}")
    assert len(rejected) == 1
    assert rejected[0].startswith("incoming/vendor_c/2026-03-05.json")
    assert "unexpected fields: UnitCost" in rejected[0]


def test_same_seed_gives_the_same_bytes(generated: Path, tmp_path: Path) -> None:
    again = tmp_path / "again"
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "generate.py"),
            "--seed",
            "42",
            "--out",
            str(again),
            "--overrides-out",
            str(tmp_path / "o.csv"),
        ],
        check=True,
        capture_output=True,
    )

    def digest(root: Path) -> str:
        h = hashlib.sha256()
        for p in sorted(root.rglob("*.json")):
            h.update(p.relative_to(root).as_posix().encode())
            h.update(p.read_bytes())
        return h.hexdigest()

    assert digest(generated) == digest(again)


def test_orders_cover_every_day_and_dates_line_up(generated: Path) -> None:
    days = {p.stem for p in (generated / "incoming" / "orders").glob("*.json")}
    assert len(days) == 30
    first = json.loads((generated / "incoming" / "orders" / "2026-03-01.json").read_text())
    assert date.fromisoformat(first["export_date"]) == date(2026, 3, 1)
