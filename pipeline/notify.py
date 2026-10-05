"""Where alerts go. Console by default; a Slack incoming webhook when SLACK_WEBHOOK_URL is set."""

from __future__ import annotations

import json
import sys
import urllib.request
from typing import Protocol

from pipeline.config import Settings


class Notifier(Protocol):
    def send(self, subject: str, body: str) -> None: ...


class ConsoleNotifier:
    def send(self, subject: str, body: str) -> None:
        print(f"[ALERT] {subject}\n{body}", file=sys.stdout, flush=True)


class SlackNotifier:
    def __init__(self, webhook_url: str, timeout: float = 5.0) -> None:
        self._url = webhook_url
        self._timeout = timeout

    def send(self, subject: str, body: str) -> None:
        payload = json.dumps({"text": f"*{subject}*\n{body}"}).encode("utf-8")
        request = urllib.request.Request(  # noqa: S310 - https webhook from env
            self._url, data=payload, headers={"Content-Type": "application/json"}, method="POST"
        )
        with urllib.request.urlopen(request, timeout=self._timeout):  # noqa: S310
            pass


class FanoutNotifier:
    """Console always, Slack in addition. A Slack outage must not fail the pipeline."""

    def __init__(self, *notifiers: Notifier) -> None:
        self._notifiers = notifiers

    def send(self, subject: str, body: str) -> None:
        for notifier in self._notifiers:
            try:
                notifier.send(subject, body)
            except Exception as exc:  # noqa: BLE001 - alerting is best effort
                print(f"[ALERT-FAILED] {type(notifier).__name__}: {exc}", file=sys.stderr)


def get_notifier(settings: Settings) -> Notifier:
    if settings.slack_webhook_url:
        return FanoutNotifier(ConsoleNotifier(), SlackNotifier(settings.slack_webhook_url))
    return ConsoleNotifier()
