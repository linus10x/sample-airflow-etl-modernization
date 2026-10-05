from __future__ import annotations

import io
import urllib.request
from datetime import date
from typing import Any

import pytest

from pipeline.config import Settings
from pipeline.freshness import check_freshness
from pipeline.notify import ConsoleNotifier, FanoutNotifier, SlackNotifier, get_notifier
from tests.conftest import FakeWarehouse, RecordingNotifier


def loaded(wh: FakeWarehouse, feed: str, day: date, tag: str) -> None:
    wh.record_loaded(
        feed=feed,
        path="p",
        checksum=tag,
        file_date=day,
        contract_version="v1",
        records=[],
        size_bytes=1,
        run_id="r",
    )


def test_freshness_flags_missing_and_lagging_feeds(warehouse: FakeWarehouse) -> None:
    loaded(warehouse, "vendor_a", date(2026, 3, 5), "a")
    loaded(warehouse, "vendor_b", date(2026, 3, 2), "b")
    results = {
        r.feed: r
        for r in check_freshness(
            warehouse, ("vendor_a", "vendor_b", "vendor_c"), as_of=date(2026, 3, 5), max_lag_days=1
        )
    }
    assert not results["vendor_a"].stale and results["vendor_a"].lag_days == 0
    assert results["vendor_b"].stale and results["vendor_b"].lag_days == 3
    assert results["vendor_c"].stale and results["vendor_c"].latest is None
    assert "no loaded file yet" in results["vendor_c"].describe()
    assert "3 day(s) behind" in results["vendor_b"].describe()


def test_freshness_ignores_files_after_the_as_of_date(warehouse: FakeWarehouse) -> None:
    loaded(warehouse, "vendor_a", date(2026, 3, 9), "late")
    (result,) = check_freshness(warehouse, ("vendor_a",), as_of=date(2026, 3, 5))
    assert result.latest is None and result.stale


def settings(webhook: str | None) -> Settings:
    return Settings("http://s3", "b", "k", "s", "postgresql://x", webhook)


def test_console_is_the_default_notifier() -> None:
    assert isinstance(get_notifier(settings(None)), ConsoleNotifier)


def test_console_notifier_prints(capsys: pytest.CaptureFixture[str]) -> None:
    ConsoleNotifier().send("subject", "body")
    assert "[ALERT] subject" in capsys.readouterr().out


def test_slack_is_added_when_a_webhook_is_configured() -> None:
    assert isinstance(get_notifier(settings("https://hooks.example.com/x")), FanoutNotifier)


def test_slack_notifier_posts_json(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class _Resp(io.BytesIO):
        def __enter__(self) -> _Resp:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    def fake_urlopen(request: urllib.request.Request, timeout: float) -> _Resp:
        captured["url"] = request.full_url
        captured["body"] = request.data
        captured["timeout"] = timeout
        return _Resp()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    SlackNotifier("https://hooks.example.com/x", timeout=2).send("Subject", "Body text")
    assert captured["url"] == "https://hooks.example.com/x"
    assert b"Subject" in captured["body"] and b"Body text" in captured["body"]
    assert captured["timeout"] == 2


def test_a_failing_channel_does_not_stop_the_others(capsys: pytest.CaptureFixture[str]) -> None:
    class Broken:
        def send(self, subject: str, body: str) -> None:
            raise OSError("webhook down")

    good = RecordingNotifier()
    FanoutNotifier(Broken(), good).send("s", "b")
    assert good.sent == [("s", "b")]
    assert "ALERT-FAILED" in capsys.readouterr().err


def test_settings_read_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("S3_BUCKET", "other-bucket")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "")
    s = Settings.from_env()
    assert s.s3_bucket == "other-bucket" and s.slack_webhook_url is None
