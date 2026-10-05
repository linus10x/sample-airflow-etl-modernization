from __future__ import annotations

from datetime import date

from pipeline.report import FeedRow, ReportData, render_html


def test_render_escapes_reasons_and_shows_the_rate() -> None:
    data = ReportData(
        feeds=[
            FeedRow("vendor_a", 3, 0, 900, date(2026, 3, 3), 0),
            FeedRow("vendor_b", 1, 1, 10, date(2026, 3, 1), 5),
            FeedRow("vendor_c"),
        ],
        rejected=[
            {
                "file_date": date(2026, 3, 5),
                "feed": "vendor_c",
                "path": "incoming/x.json",
                "reason": "<script>alert(1)</script> bad",
            }
        ],
        mart_counts={"fct_orders": 5000, "dim_products": None},
        sku_match=[
            {
                "match_method": "exact_normalized",
                "line_count": 90,
                "distinct_skus": 9,
                "pct_of_lines": 90.0,
            },
            {
                "match_method": "unmatched",
                "line_count": 10,
                "distinct_skus": 3,
                "pct_of_lines": 10.0,
            },
        ],
        as_of=date(2026, 3, 3),
    )
    page = render_html(data)
    assert "<script>" not in page and "&lt;script&gt;" in page
    assert data.match_rate_pct == 90.0 and "90.0%" in page
    assert page.count('class="pill stale"') == 1 and page.count('class="pill fresh"') == 1
    assert "no data" in page and "5,000" in page and "not built yet" in page


def test_empty_report_renders() -> None:
    page = render_html(ReportData())
    assert "No rejected files." in page and "none" in page
