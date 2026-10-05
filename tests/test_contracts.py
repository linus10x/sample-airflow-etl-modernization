"""One test per contract rule. A rule with no test is a rule that can silently stop working."""

from __future__ import annotations

import copy
from datetime import date
from typing import Any

import pytest

from pipeline.contracts import CONTRACTS, MAX_PROBLEMS_SHOWN, validate_file
from pipeline.errors import FileRejected
from tests.conftest import (
    orders_payload,
    vendor_a_payload,
    vendor_b_payload,
    vendor_c_payload,
)

DAY = date(2026, 3, 1)


def reject(feed: str, payload: Any, day: date = DAY) -> FileRejected:
    with pytest.raises(FileRejected) as info:
        validate_file(feed, payload, day)
    return info.value


@pytest.mark.parametrize(
    ("feed", "payload"),
    [
        ("vendor_a", vendor_a_payload()),
        ("vendor_b", vendor_b_payload()),
        ("vendor_c", vendor_c_payload()),
        ("orders", orders_payload()),
    ],
)
def test_valid_files_return_their_raw_records(feed: str, payload: dict[str, Any]) -> None:
    records = validate_file(feed, payload, DAY)
    assert records == payload[CONTRACTS[feed].records_field]


def test_every_feed_has_a_versioned_contract() -> None:
    assert set(CONTRACTS) == {"vendor_a", "vendor_b", "vendor_c", "orders"}
    assert all(c.version.startswith("v") for c in CONTRACTS.values())


def test_unknown_feed_is_rejected() -> None:
    assert reject("vendor_z", {}).code == "unknown_feed"


@pytest.mark.parametrize("payload", [[], "text", 5, None])
def test_top_level_must_be_an_object(payload: Any) -> None:
    err = reject("vendor_a", payload)
    assert err.code == "contract_violation"
    assert "top level must be a JSON object" in err.reason


def test_renamed_field_is_named_with_the_field_that_showed_up_instead() -> None:
    err = reject("vendor_c", vendor_c_payload(cost_field="UnitCost"))
    assert err.code == "contract_violation"
    assert "missing required field 'Cost'" in err.reason
    assert "unexpected fields: UnitCost" in err.reason
    assert "2 of 2 rows" in err.reason  # collapsed into one line, with a count


def test_extra_fields_a_vendor_adds_are_tolerated() -> None:
    payload = vendor_a_payload()
    payload["items"][0]["warehouse_bin"] = "A-4"
    payload["note"] = "new top level field"
    assert len(validate_file("vendor_a", payload, DAY)) == 3


def test_wrong_vendor_id_in_envelope() -> None:
    payload = vendor_a_payload()
    payload["vendor"] = "vendor_b"
    assert "vendor" in reject("vendor_a", payload).reason


def test_sku_format_is_enforced() -> None:
    payload = vendor_a_payload()
    payload["items"][1]["sku"] = "AC-1"
    assert "sku" in reject("vendor_a", payload).reason


def test_negative_cost_is_rejected() -> None:
    payload = vendor_b_payload()
    payload["products"][0]["unit_cost"] = "-1.00"
    assert "unit_cost" in reject("vendor_b", payload).reason


def test_cost_with_more_than_two_decimals_is_rejected() -> None:
    payload = vendor_a_payload()
    payload["items"][0]["cost_price"] = 1.234
    assert "cost_price" in reject("vendor_a", payload).reason


def test_negative_stock_is_rejected() -> None:
    payload = vendor_c_payload()
    payload["rows"][0]["Inventory"] = -3
    assert "Inventory" in reject("vendor_c", payload).reason


def test_empty_item_list_is_rejected() -> None:
    payload = vendor_a_payload()
    payload["items"] = []
    assert "items" in reject("vendor_a", payload).reason


@pytest.mark.parametrize(
    ("feed", "payload_fn", "list_key"),
    [
        ("vendor_a", vendor_a_payload, "items"),
        ("vendor_b", vendor_b_payload, "products"),
        ("vendor_c", vendor_c_payload, "rows"),
    ],
)
def test_duplicate_skus_in_one_file_are_rejected(feed: str, payload_fn: Any, list_key: str) -> None:
    payload = payload_fn()
    payload[list_key].append(copy.deepcopy(payload[list_key][0]))
    assert "duplicate" in reject(feed, payload).reason


def test_file_name_date_must_match_the_date_inside() -> None:
    err = reject("vendor_a", vendor_a_payload(day="2026-03-02"))
    assert err.code == "date_mismatch"
    assert "2026-03-02" in err.reason
    assert "2026-03-01" in err.reason


def test_order_id_format_is_enforced() -> None:
    payload = orders_payload()
    payload["orders"][0]["order_id"] = "ORDER-1"
    assert "order_id" in reject("orders", payload).reason


def test_order_needs_at_least_one_line() -> None:
    payload = orders_payload()
    payload["orders"][0]["lines"] = []
    assert "lines" in reject("orders", payload).reason


def test_order_line_quantity_must_be_positive() -> None:
    payload = orders_payload()
    payload["orders"][0]["lines"][0]["qty"] = 0
    assert "qty" in reject("orders", payload).reason


def test_order_line_vendor_must_be_known() -> None:
    payload = orders_payload()
    payload["orders"][0]["lines"][0]["vendor"] = "vendor_q"
    assert "vendor" in reject("orders", payload).reason


def test_duplicate_order_ids_are_rejected() -> None:
    payload = orders_payload()
    payload["orders"].append(copy.deepcopy(payload["orders"][0]))
    assert "duplicate order_id" in reject("orders", payload).reason


def test_order_placed_outside_the_export_day_is_rejected() -> None:
    payload = orders_payload()
    payload["orders"][0]["placed_at"] = "2026-03-02T01:00:00"
    assert "outside export_date" in reject("orders", payload).reason


def test_long_problem_lists_are_truncated_with_a_count() -> None:
    payload = vendor_a_payload()
    for field in ("sku", "name", "cost_price", "qty_on_hand"):
        payload["items"][0].pop(field)
    payload["items"][1]["sku"] = "bad"
    payload["items"][1]["qty_on_hand"] = -1
    payload["items"][2]["cost_price"] = -5
    payload["vendor"] = "vendor_x"
    err = reject("vendor_a", payload)
    assert "more problem(s)" in err.reason
    assert err.reason.count(";") <= MAX_PROBLEMS_SHOWN
