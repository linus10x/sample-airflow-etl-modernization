"""Command line entry points: `python -m pipeline.cli <command>`, same functions as the DAGs."""

from __future__ import annotations

import argparse
import sys
import time
import uuid
from datetime import date
from pathlib import Path

from pipeline.config import Settings
from pipeline.feeds import FEEDS
from pipeline.freshness import check_freshness
from pipeline.ingest import ingest_feed
from pipeline.notify import get_notifier
from pipeline.storage import S3Store
from pipeline.warehouse import Warehouse


def _day(text: str) -> date:
    return date.fromisoformat(text)


def wait_for_s3(store: S3Store, attempts: int = 30, delay: float = 1.0) -> None:
    for attempt in range(attempts):
        try:
            store.ensure_bucket()
            return
        except Exception:  # noqa: BLE001 - any connection error means "not up yet"
            if attempt == attempts - 1:
                raise
            time.sleep(delay)


def cmd_upload(args: argparse.Namespace, settings: Settings) -> int:
    store = S3Store.from_settings(settings)
    wait_for_s3(store)
    root: Path = args.source
    count = 0
    for path in sorted((root / "incoming").rglob("*.json")):
        store.put(path.relative_to(root).as_posix(), path.read_bytes())
        count += 1
    print(f"uploaded {count} files to s3://{settings.s3_bucket}/incoming/")
    return 0


def cmd_reset(args: argparse.Namespace, settings: Settings) -> int:
    """Return the stack to a clean slate: empty warehouse, no quarantined objects."""
    Warehouse(settings.warehouse_dsn).reset()
    removed = S3Store.from_settings(settings).delete_prefix("quarantine/")
    print(f"warehouse schemas dropped; {removed} quarantine objects removed")
    return 0


def cmd_init_db(_: argparse.Namespace, settings: Settings) -> int:
    Warehouse(settings.warehouse_dsn).ensure_schema()
    print("schemas raw and ops are ready")
    return 0


def cmd_ingest(args: argparse.Namespace, settings: Settings) -> int:
    store = S3Store.from_settings(settings)
    warehouse = Warehouse(settings.warehouse_dsn)
    warehouse.ensure_schema()
    notifier = get_notifier(settings)
    run_id = args.run_id or f"cli-{uuid.uuid4().hex[:8]}"
    feeds = args.feed or list(FEEDS)
    quarantined = 0
    for feed in feeds:
        result = ingest_feed(
            store, warehouse, feed, start=args.start, end=args.end, run_id=run_id, notifier=notifier
        )
        print(result.summary())
        quarantined += len(result.quarantined)
    return 1 if (quarantined and args.fail_on_quarantine) else 0


def cmd_freshness(args: argparse.Namespace, settings: Settings) -> int:
    results = check_freshness(
        Warehouse(settings.warehouse_dsn), FEEDS, args.as_of, args.max_lag_days
    )
    for r in results:
        print(("STALE  " if r.stale else "fresh  ") + r.describe())
    return 1 if any(r.stale for r in results) else 0


def cmd_report(args: argparse.Namespace, settings: Settings) -> int:
    from pipeline.report import collect, render_html

    html = render_html(collect(Warehouse(settings.warehouse_dsn)))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(html, encoding="utf-8")
    print(f"wrote {args.out}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    up = sub.add_parser("upload", help="put generated files into the S3 bucket")
    up.add_argument("--source", type=Path, default=Path("data/generated"))
    up.set_defaults(func=cmd_upload)

    sub.add_parser("init-db", help="create the raw and ops schemas").set_defaults(func=cmd_init_db)
    sub.add_parser("reset", help="drop warehouse schemas and quarantine objects").set_defaults(
        func=cmd_reset
    )

    ing = sub.add_parser("ingest", help="validate and load files")
    ing.add_argument("--feed", action="append", choices=FEEDS)
    ing.add_argument("--start", type=_day)
    ing.add_argument("--end", type=_day)
    ing.add_argument("--run-id")
    ing.add_argument("--fail-on-quarantine", action="store_true")
    ing.set_defaults(func=cmd_ingest)

    fr = sub.add_parser("freshness", help="exit 1 if a feed is stale")
    fr.add_argument("--as-of", type=_day, required=True)
    fr.add_argument("--max-lag-days", type=int, default=1)
    fr.set_defaults(func=cmd_freshness)

    rep = sub.add_parser("report", help="render the pipeline health report")
    rep.add_argument("--out", type=Path, default=Path("out/health_report.html"))
    rep.set_defaults(func=cmd_report)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args, Settings.from_env()))


if __name__ == "__main__":
    sys.exit(main())
