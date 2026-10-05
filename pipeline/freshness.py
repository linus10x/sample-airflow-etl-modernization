"""SLA-style freshness check: each feed must have a loaded file no older than `max_lag_days`."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Protocol


class _Reader(Protocol):
    def latest_loaded_date(self, feed: str, up_to: date) -> date | None: ...


@dataclass(frozen=True)
class FeedFreshness:
    feed: str
    latest: date | None
    lag_days: int | None
    stale: bool

    def describe(self) -> str:
        if self.latest is None:
            return f"{self.feed}: no loaded file yet"
        return f"{self.feed}: latest file {self.latest}, {self.lag_days} day(s) behind"


def check_freshness(
    reader: _Reader, feeds: tuple[str, ...], as_of: date, max_lag_days: int = 1
) -> list[FeedFreshness]:
    results: list[FeedFreshness] = []
    for feed in feeds:
        latest = reader.latest_loaded_date(feed, as_of)
        if latest is None:
            results.append(FeedFreshness(feed, None, None, True))
            continue
        lag = (as_of - latest).days
        results.append(FeedFreshness(feed, latest, lag, lag > max_lag_days))
    return results
