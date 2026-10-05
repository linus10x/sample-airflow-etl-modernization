#!/usr/bin/env python3
"""Collect the numbers the case study and the manifest may quote, from test and build output only.

Inputs are files CI produces: pytest JUnit XML (any number of them), coverage.xml, the dbt
run_results.json from the transform DAG, and out/report_facts.json written by
`python -m pipeline.cli report --facts`. Nothing here is typed in by hand.
"""

from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def junit_counts(path: Path) -> dict[str, int]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
    total = sum(int(s.get("tests", 0)) for s in suites)
    bad = sum(int(s.get("failures", 0)) + int(s.get("errors", 0)) for s in suites)
    skipped = sum(int(s.get("skipped", 0)) for s in suites)
    return {"tests": total, "failed": bad, "skipped": skipped, "passed": total - bad - skipped}


def coverage_pct(path: Path) -> float:
    return round(float(ET.parse(path).getroot().get("line-rate", 0)) * 100, 1)


def dbt_counts(path: Path) -> dict[str, int]:
    results = json.loads(path.read_text())["results"]
    ok = sum(1 for r in results if r["status"] in {"pass", "success"})
    checks = [r for r in results if r["unique_id"].split(".")[0] in {"test", "unit_test"}]
    return {
        "all_nodes": len(results),
        "all_ok": ok,
        "checks": len(checks),
        "checks_passed": sum(1 for r in checks if r["status"] == "pass"),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--junit", nargs="+", type=Path, required=True)
    parser.add_argument("--coverage", type=Path, required=True)
    parser.add_argument("--dbt", type=Path, required=True)
    parser.add_argument("--report-facts", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    per_file = {p.name: junit_counts(p) for p in args.junit}
    failed = sum(c["failed"] for c in per_file.values())
    dbt = dbt_counts(args.dbt)
    report = json.loads(args.report_facts.read_text())
    facts = {
        "pytest_total": sum(c["passed"] for c in per_file.values()),
        "pytest_failed": failed,
        "pytest_by_suite": {k: v["passed"] for k, v in per_file.items()},
        "coverage_pct": coverage_pct(args.coverage),
        "dbt_total": dbt["checks_passed"],
        "dbt_failed": dbt["checks"] - dbt["checks_passed"],
        "orders": report["mart_counts"]["fct_orders"],
        "order_lines": report["mart_counts"]["fct_order_lines"],
        "match_pct": report["match_rate_pct"],
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(facts, indent=2) + "\n")
    print(json.dumps(facts))
    return 1 if facts["pytest_failed"] or facts["dbt_failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
