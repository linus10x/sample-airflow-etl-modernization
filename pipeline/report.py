"""Pipeline health report: freshness, row counts, rejected files and the SKU match rate.

`collect` reads the ops and marts schemas. `render_html` is a pure function of its output, so the
page can be tested without a database. Freshness is measured against the newest file in the
manifest rather than the wall clock, because the sample's files are dated in the past.
"""

from __future__ import annotations

import html
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Protocol

import psycopg.errors

from pipeline.feeds import FEEDS

MART_TABLES = ("dim_products", "crosswalk_sku", "fct_order_lines", "fct_orders")
MAX_LAG_DAYS = 1


class _Reader(Protocol):
    def fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]: ...


@dataclass
class FeedRow:
    feed: str
    files_loaded: int = 0
    files_quarantined: int = 0
    rows_loaded: int = 0
    latest_file_date: date | None = None
    lag_days: int | None = None

    @property
    def status(self) -> str:
        if self.latest_file_date is None:
            return "no data"
        return "stale" if (self.lag_days or 0) > MAX_LAG_DAYS else "fresh"


@dataclass
class ReportData:
    feeds: list[FeedRow] = field(default_factory=list)
    rejected: list[dict[str, Any]] = field(default_factory=list)
    mart_counts: dict[str, int | None] = field(default_factory=dict)
    sku_match: list[dict[str, Any]] | None = None
    as_of: date | None = None

    @property
    def match_rate_pct(self) -> float | None:
        if not self.sku_match:
            return None
        total = sum(int(r["line_count"]) for r in self.sku_match)
        matched = sum(
            int(r["line_count"]) for r in self.sku_match if r["match_method"] != "unmatched"
        )
        return round(100.0 * matched / total, 1) if total else None


def collect(reader: _Reader) -> ReportData:
    rows = reader.fetch_all(
        """
        select feed,
               count(*) filter (where status = 'loaded')       as files_loaded,
               count(*) filter (where status = 'quarantined')  as files_quarantined,
               coalesce(sum(row_count) filter (where status = 'loaded'), 0) as rows_loaded,
               max(file_date) filter (where status = 'loaded') as latest_file_date
        from ops.file_manifest
        group by feed
        """
    )
    by_feed = {r["feed"]: r for r in rows}
    data = ReportData()
    for feed in FEEDS:
        r = by_feed.get(feed)
        data.feeds.append(
            FeedRow(
                feed,
                int(r["files_loaded"]),
                int(r["files_quarantined"]),
                int(r["rows_loaded"]),
                r["latest_file_date"],
            )
            if r
            else FeedRow(feed)
        )
    dates = [f.latest_file_date for f in data.feeds if f.latest_file_date]
    data.as_of = max(dates) if dates else None
    for f in data.feeds:
        if f.latest_file_date and data.as_of:
            f.lag_days = (data.as_of - f.latest_file_date).days

    data.rejected = reader.fetch_all(
        """
        select feed, path, file_date, reason_code, reason, processed_at
        from ops.file_manifest where status = 'quarantined'
        order by file_date, path
        """
    )
    for table in MART_TABLES:
        data.mart_counts[table] = _try_count(reader, f"marts.{table}")
    try:
        data.sku_match = reader.fetch_all(
            "select match_method, line_count, distinct_skus, pct_of_lines "
            "from marts.sku_match_summary order by match_method"
        )
    except psycopg.errors.UndefinedTable:
        data.sku_match = None
    return data


def _try_count(reader: _Reader, table: str) -> int | None:
    try:
        return int(reader.fetch_all(f"select count(*) as n from {table}")[0]["n"])  # noqa: S608
    except psycopg.errors.UndefinedTable:
        return None


def _e(value: object) -> str:
    return html.escape("" if value is None else str(value))


def _fmt(n: int | None) -> str:
    return "not built yet" if n is None else f"{n:,}"


def render_html(data: ReportData) -> str:
    quarantined = sum(f.files_quarantined for f in data.feeds)
    loaded = sum(f.files_loaded for f in data.feeds)
    rate = data.match_rate_pct
    cards = [
        ("Files loaded", f"{loaded:,}"),
        ("Files quarantined", f"{quarantined:,}"),
        ("Newest file", _e(data.as_of) or "none"),
        ("Order lines tied to a product", "n/a" if rate is None else f"{rate}%"),
    ]
    card_html = "".join(
        f'<div class="card"><div class="v">{v}</div><div class="k">{k}</div></div>'
        for k, v in cards
    )
    feed_rows = "".join(
        f'<tr><td>{_e(f.feed)}</td><td><span class="pill {f.status.replace(" ", "-")}">{f.status}</span></td>'
        f"<td>{_e(f.latest_file_date) or '-'}</td><td>{'-' if f.lag_days is None else f.lag_days}</td>"
        f'<td class="n">{f.files_loaded}</td><td class="n">{f.files_quarantined}</td>'
        f'<td class="n">{f.rows_loaded:,}</td></tr>'
        for f in data.feeds
    )
    if data.rejected:
        rejected_rows = "".join(
            f"<tr><td>{_e(r['file_date'])}</td><td>{_e(r['feed'])}</td><td class='mono'>{_e(r['path'])}</td>"
            f"<td>{_e(r['reason'])}</td></tr>"
            for r in data.rejected
        )
    else:
        rejected_rows = '<tr><td colspan="4">No rejected files.</td></tr>'
    mart_rows = "".join(
        f"<tr><td class='mono'>marts.{t}</td><td class='n'>{_fmt(n)}</td></tr>"
        for t, n in data.mart_counts.items()
    )
    if data.sku_match:
        match_rows = "".join(
            f"<tr><td>{_e(r['match_method'])}</td><td class='n'>{int(r['line_count']):,}</td>"
            f"<td class='n'>{int(r['distinct_skus']):,}</td><td class='n'>{_e(r['pct_of_lines'])}%</td></tr>"
            for r in data.sku_match
        )
    else:
        match_rows = '<tr><td colspan="4">Run dbt to build the crosswalk.</td></tr>'

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pipeline health report</title>
<style>
:root {{ --navy:#14213d; --amber:#e8a317; --ink:#1d2433; --mute:#5b6577; --line:#dfe3ea; --bg:#f6f7f9; }}
* {{ box-sizing: border-box; }}
body {{ margin:0; font:15px/1.45 -apple-system,Segoe UI,Helvetica,Arial,sans-serif; color:var(--ink); background:var(--bg); }}
header {{ background:var(--navy); color:#fff; padding:22px 32px; border-bottom:4px solid var(--amber); }}
header h1 {{ margin:0 0 4px; font-size:22px; }}
header p {{ margin:0; color:#c9d0de; font-size:13px; }}
main {{ padding:22px 32px 32px; max-width:1180px; margin:0 auto; }}
.cards {{ display:grid; grid-template-columns:repeat(4,1fr); gap:14px; margin-bottom:22px; }}
.card {{ background:#fff; border:1px solid var(--line); border-radius:8px; padding:14px 16px; }}
.card .v {{ font-size:26px; font-weight:650; color:var(--navy); }}
.card .k {{ font-size:12px; color:var(--mute); text-transform:uppercase; letter-spacing:.04em; }}
h2 {{ font-size:15px; margin:22px 0 8px; color:var(--navy); }}
table {{ width:100%; border-collapse:collapse; background:#fff; border:1px solid var(--line); border-radius:8px; overflow:hidden; font-size:13.5px; }}
th,td {{ text-align:left; padding:8px 12px; border-bottom:1px solid var(--line); vertical-align:top; }}
th {{ background:#eef1f6; font-size:12px; text-transform:uppercase; letter-spacing:.04em; color:var(--mute); }}
td.n {{ text-align:right; font-variant-numeric:tabular-nums; }}
.mono {{ font-family:ui-monospace,Menlo,Consolas,monospace; font-size:12.5px; }}
.pill {{ padding:2px 8px; border-radius:10px; font-size:12px; font-weight:600; }}
.pill.fresh {{ background:#dcf3e4; color:#17653a; }} .pill.stale,.pill.no-data {{ background:#fde4e1; color:#a02419; }}
footer {{ color:var(--mute); font-size:12px; margin-top:18px; }}
</style></head><body>
<header><h1>Pipeline health report</h1>
<p>Synthetic retail data. Built from the ops and marts schemas.</p></header>
<main>
<div class="cards">{card_html}</div>
<h2>Freshness and volume by feed</h2>
<table><thead><tr><th>Feed</th><th>Status</th><th>Latest file</th><th>Days behind newest</th><th>Files loaded</th><th>Quarantined</th><th>Rows loaded</th></tr></thead><tbody>{feed_rows}</tbody></table>
<h2>Rejected files</h2>
<table><thead><tr><th>File date</th><th>Feed</th><th>File</th><th>Reason</th></tr></thead><tbody>{rejected_rows}</tbody></table>
<h2>SKU crosswalk: how order lines tied to products</h2>
<table><thead><tr><th>Match method</th><th>Order lines</th><th>Distinct SKUs as typed</th><th>Share of lines</th></tr></thead><tbody>{match_rows}</tbody></table>
<h2>Mart row counts</h2>
<table><thead><tr><th>Table</th><th>Rows</th></tr></thead><tbody>{mart_rows}</tbody></table>
<footer>Freshness is measured against the newest file in the manifest, not the clock.</footer>
</main></body></html>
"""
