"""Feed names and the S3 key layout: incoming/<feed>/<YYYY-MM-DD>.json."""

from __future__ import annotations

import re
from datetime import date

VENDOR_FEEDS: tuple[str, ...] = ("vendor_a", "vendor_b", "vendor_c")
FEEDS: tuple[str, ...] = (*VENDOR_FEEDS, "orders")

INCOMING_PREFIX = "incoming"
QUARANTINE_PREFIX = "quarantine"

_KEY_RE = re.compile(r"^incoming/(?P<feed>[a-z_]+)/(?P<day>\d{4}-\d{2}-\d{2})\.json$")


def feed_prefix(feed: str) -> str:
    return f"{INCOMING_PREFIX}/{feed}/"


def incoming_key(feed: str, day: date) -> str:
    return f"{INCOMING_PREFIX}/{feed}/{day.isoformat()}.json"


def parse_incoming_key(key: str) -> tuple[str, date] | None:
    """Return (feed, file date) for a well-formed key, else None."""
    match = _KEY_RE.match(key)
    if match is None:
        return None
    try:
        return match["feed"], date.fromisoformat(match["day"])
    except ValueError:
        return None


def quarantine_key(key: str) -> str:
    if key.startswith(f"{INCOMING_PREFIX}/"):
        return f"{QUARANTINE_PREFIX}/{key[len(INCOMING_PREFIX) + 1 :]}"
    return f"{QUARANTINE_PREFIX}/{key}"


def reason_key(key: str) -> str:
    return f"{quarantine_key(key)}.reason.json"
