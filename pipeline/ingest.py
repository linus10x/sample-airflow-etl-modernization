"""Discover, check and load the files for one feed.

The order of operations for each file is the whole safety story:
  1. read the bytes and hash them
  2. if the manifest already has the hash, skip (a re-run loads nothing)
  3. parse and check against the feed's contract
  4. valid: insert manifest row and records in one transaction
     invalid: copy to quarantine/ with a reason file, write a manifest row, alert, keep going
A bad file never stops the other files, and never reaches the raw tables.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Protocol

from pipeline.contracts import CONTRACTS, validate_file
from pipeline.errors import FileRejected
from pipeline.feeds import feed_prefix, parse_incoming_key, quarantine_key, reason_key
from pipeline.notify import Notifier
from pipeline.parsing import parse_json_bytes, sha256_hex
from pipeline.storage import ObjectStore


class _Warehouse(Protocol):
    def record_loaded(
        self,
        *,
        feed: str,
        path: str,
        checksum: str,
        file_date: date,
        contract_version: str,
        records: list[dict[str, Any]],
        size_bytes: int,
        run_id: str,
    ) -> bool: ...

    def record_quarantined(
        self,
        *,
        feed: str,
        path: str,
        checksum: str,
        file_date: date,
        reason_code: str,
        reason: str,
        size_bytes: int,
        run_id: str,
    ) -> bool: ...


@dataclass(frozen=True)
class FileOutcome:
    key: str
    status: str  # loaded | quarantined | skipped
    checksum: str
    rows: int = 0
    reason: str | None = None


@dataclass
class IngestResult:
    feed: str
    outcomes: list[FileOutcome] = field(default_factory=list)

    def _with(self, status: str) -> list[FileOutcome]:
        return [o for o in self.outcomes if o.status == status]

    @property
    def loaded(self) -> list[FileOutcome]:
        return self._with("loaded")

    @property
    def quarantined(self) -> list[FileOutcome]:
        return self._with("quarantined")

    @property
    def skipped(self) -> list[FileOutcome]:
        return self._with("skipped")

    @property
    def rows_loaded(self) -> int:
        return sum(o.rows for o in self.loaded)

    def summary(self) -> str:
        return (
            f"{self.feed}: {len(self.loaded)} loaded ({self.rows_loaded} rows), "
            f"{len(self.quarantined)} quarantined, {len(self.skipped)} already loaded"
        )


def discover(
    store: ObjectStore, feed: str, start: date | None = None, end: date | None = None
) -> list[tuple[str, date | None]]:
    """List incoming keys for a feed inside [start, end]. Badly named keys are returned with
    a None date so they get quarantined instead of silently ignored."""
    found: list[tuple[str, date | None]] = []
    for key in store.list_keys(feed_prefix(feed)):
        parsed = parse_incoming_key(key)
        if parsed is None:
            found.append((key, None))
            continue
        _, day = parsed
        if (start is None or day >= start) and (end is None or day <= end):
            found.append((key, day))
    return found


def ingest_feed(
    store: ObjectStore,
    warehouse: _Warehouse,
    feed: str,
    *,
    start: date | None = None,
    end: date | None = None,
    run_id: str = "manual",
    notifier: Notifier | None = None,
    on_file: Callable[[FileOutcome], None] | None = None,
) -> IngestResult:
    if feed not in CONTRACTS:
        raise ValueError(f"unknown feed {feed!r}")
    result = IngestResult(feed=feed)
    for key, day in discover(store, feed, start, end):
        outcome = _ingest_one(store, warehouse, feed, key, day, run_id)
        result.outcomes.append(outcome)
        if on_file:
            on_file(outcome)
        if outcome.status == "quarantined" and notifier is not None:
            notifier.send(
                f"File quarantined: {key}",
                f"{outcome.reason}\nThe other files in this run were not affected.",
            )
    return result


def _ingest_one(
    store: ObjectStore,
    warehouse: _Warehouse,
    feed: str,
    key: str,
    day: date | None,
    run_id: str,
) -> FileOutcome:
    data = store.get(key)
    checksum = sha256_hex(data)
    # A file with an unreadable name still needs a date for the manifest. Use a sentinel.
    file_date = day or date(1970, 1, 1)
    try:
        if day is None:
            raise FileRejected(
                "unexpected_key", f"{key!r} does not match incoming/<feed>/<YYYY-MM-DD>.json"
            )
        payload = parse_json_bytes(data)
        records = validate_file(feed, payload, day)
    except FileRejected as rejected:
        written = warehouse.record_quarantined(
            feed=feed,
            path=key,
            checksum=checksum,
            file_date=file_date,
            reason_code=rejected.code,
            reason=rejected.reason,
            size_bytes=len(data),
            run_id=run_id,
        )
        if not written:
            return FileOutcome(key, "skipped", checksum)
        store.copy(key, quarantine_key(key))
        store.put(
            reason_key(key),
            json.dumps(
                {
                    "key": key,
                    "checksum": checksum,
                    "code": rejected.code,
                    "reason": rejected.reason,
                },
                indent=2,
            ).encode("utf-8"),
        )
        return FileOutcome(key, "quarantined", checksum, reason=rejected.reason)

    loaded = warehouse.record_loaded(
        feed=feed,
        path=key,
        checksum=checksum,
        file_date=file_date,
        contract_version=CONTRACTS[feed].version,
        records=records,
        size_bytes=len(data),
        run_id=run_id,
    )
    if not loaded:
        return FileOutcome(key, "skipped", checksum)
    return FileOutcome(key, "loaded", checksum, rows=len(records))
