from __future__ import annotations

import json
from datetime import date
from typing import Any

import pytest

from pipeline.storage import MemoryStore

pytest_plugins = ["tests.services"]


class FakeWarehouse:
    """In-memory stand-in with the same checksum-gated behavior as the Postgres Warehouse."""

    def __init__(self) -> None:
        self.manifest: dict[str, dict[str, Any]] = {}
        self.records: dict[str, list[dict[str, Any]]] = {}

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
    ) -> bool:
        if checksum in self.manifest:
            return False
        self.manifest[checksum] = {
            "feed": feed,
            "path": path,
            "status": "loaded",
            "file_date": file_date,
            "run_id": run_id,
        }
        self.records[checksum] = records
        return True

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
    ) -> bool:
        if checksum in self.manifest:
            return False
        self.manifest[checksum] = {
            "feed": feed,
            "path": path,
            "status": "quarantined",
            "file_date": file_date,
            "reason_code": reason_code,
            "reason": reason,
            "run_id": run_id,
        }
        return True

    def latest_loaded_date(self, feed: str, up_to: date) -> date | None:
        days = [
            m["file_date"]
            for m in self.manifest.values()
            if m["feed"] == feed and m["status"] == "loaded" and m["file_date"] <= up_to
        ]
        return max(days) if days else None


class RecordingNotifier:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []

    def send(self, subject: str, body: str) -> None:
        self.sent.append((subject, body))


def vendor_a_payload(day: str = "2026-03-01", n: int = 3) -> dict[str, Any]:
    return {
        "vendor": "vendor_a",
        "snapshot_date": day,
        "items": [
            {
                "sku": f"AC-{10000 + i}",
                "name": f"Item {i}",
                "cost_price": 10.5 + i,
                "qty_on_hand": i,
            }
            for i in range(n)
        ],
    }


def vendor_b_payload(day: str = "2026-03-01") -> dict[str, Any]:
    return {
        "supplier": "vendor_b",
        "as_of": day,
        "products": [
            {"item_code": "b_10001_x", "title": "Thing", "unit_cost": "4.20", "stock": 7},
            {"item_code": "b_10002", "title": "Other", "unit_cost": "9.99", "stock": 0},
        ],
    }


def vendor_c_payload(day: str = "2026-03-01", cost_field: str = "Cost") -> dict[str, Any]:
    return {
        "feed": "vendor_c",
        "date": day,
        "rows": [
            {"ProductCode": "C0010001", "Description": "Thing", cost_field: 3.5, "Inventory": 4},
            {"ProductCode": "C0010002", "Description": "Other", cost_field: 8.0, "Inventory": 9},
        ],
    }


def orders_payload(day: str = "2026-03-01") -> dict[str, Any]:
    return {
        "export_date": day,
        "orders": [
            {
                "order_id": "O-000001",
                "placed_at": f"{day}T09:30:00",
                "customer_ref": "CUST-0001",
                "lines": [
                    {
                        "line_no": 1,
                        "vendor": "vendor_a",
                        "sku": "ac 10001",
                        "qty": 2,
                        "unit_price": 19.99,
                    },
                    {
                        "line_no": 2,
                        "vendor": "vendor_c",
                        "sku": "10002",
                        "qty": 1,
                        "unit_price": 12.5,
                    },
                ],
            }
        ],
    }


def as_bytes(payload: Any) -> bytes:
    return json.dumps(payload).encode("utf-8")


@pytest.fixture
def warehouse() -> FakeWarehouse:
    return FakeWarehouse()


@pytest.fixture
def notifier() -> RecordingNotifier:
    return RecordingNotifier()


@pytest.fixture
def store() -> MemoryStore:
    return MemoryStore()
