"""Data contracts: one versioned pydantic model per feed.

A file either satisfies its contract or is rejected whole, with a reason a person can act on.
Extra fields a vendor adds are tolerated; a missing or renamed required field is not, and the
reason names the unexpected fields that showed up in its place.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal, get_args

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from pipeline.errors import FileRejected

MAX_PROBLEMS_SHOWN = 5


class _Record(BaseModel):
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)


def _money() -> Any:
    return Field(ge=0, allow_inf_nan=False, max_digits=12, decimal_places=2)


# vendor_a: flat items, prices as numbers


class VendorAItemV1(_Record):
    sku: str = Field(pattern=r"^AC-\d{5}$")
    name: str = Field(min_length=1)
    cost_price: Decimal = _money()
    qty_on_hand: int = Field(ge=0)


class VendorAFileV1(_Record):
    vendor: Literal["vendor_a"]
    snapshot_date: date
    items: list[VendorAItemV1] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique_skus(self) -> VendorAFileV1:
        _reject_duplicates([i.sku for i in self.items], "sku")
        return self


# vendor_b: different field names, prices as strings


class VendorBProductV1(_Record):
    item_code: str = Field(pattern=r"^b_\d{5}(_[a-z])?$")
    title: str = Field(min_length=1)
    unit_cost: Decimal = _money()
    stock: int = Field(ge=0)


class VendorBFileV1(_Record):
    supplier: Literal["vendor_b"]
    as_of: date
    products: list[VendorBProductV1] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique_codes(self) -> VendorBFileV1:
        _reject_duplicates([p.item_code for p in self.products], "item_code")
        return self


# vendor_c: PascalCase fields, zero padded codes


class VendorCRowV1(_Record):
    ProductCode: str = Field(pattern=r"^C\d{7}$")
    Description: str = Field(min_length=1)
    Cost: Decimal = _money()
    Inventory: int = Field(ge=0)


class VendorCFileV1(_Record):
    feed: Literal["vendor_c"]
    date: date
    rows: list[VendorCRowV1] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique_codes(self) -> VendorCFileV1:
        _reject_duplicates([r.ProductCode for r in self.rows], "ProductCode")
        return self


# internal orders export


class OrderLineV1(_Record):
    line_no: int = Field(ge=1)
    vendor: Literal["vendor_a", "vendor_b", "vendor_c"]
    sku: str = Field(min_length=1)
    qty: int = Field(gt=0)
    unit_price: Decimal = _money()


class OrderV1(_Record):
    order_id: str = Field(pattern=r"^O-\d{6}$")
    placed_at: datetime
    customer_ref: str = Field(min_length=1)
    lines: list[OrderLineV1] = Field(min_length=1)


class OrdersFileV1(_Record):
    export_date: date
    orders: list[OrderV1] = Field(min_length=1)

    @model_validator(mode="after")
    def _consistent(self) -> OrdersFileV1:
        _reject_duplicates([o.order_id for o in self.orders], "order_id")
        for order in self.orders:
            if order.placed_at.date() != self.export_date:
                raise ValueError(
                    f"order {order.order_id} was placed on {order.placed_at.date()}, "
                    f"outside export_date {self.export_date}"
                )
        return self


def _reject_duplicates(values: list[str], field: str) -> None:
    seen: set[str] = set()
    for value in values:
        if value in seen:
            raise ValueError(f"duplicate {field} {value!r} in one file")
        seen.add(value)


@dataclass(frozen=True)
class Contract:
    feed: str
    version: str
    model: type[BaseModel]
    records_field: str
    date_field: str


CONTRACTS: dict[str, Contract] = {
    "vendor_a": Contract("vendor_a", "v1", VendorAFileV1, "items", "snapshot_date"),
    "vendor_b": Contract("vendor_b", "v1", VendorBFileV1, "products", "as_of"),
    "vendor_c": Contract("vendor_c", "v1", VendorCFileV1, "rows", "date"),
    "orders": Contract("orders", "v1", OrdersFileV1, "orders", "export_date"),
}


def validate_file(feed: str, payload: Any, file_date: date) -> list[dict[str, Any]]:
    """Return the raw records of a valid file. Raise FileRejected with a readable reason."""
    contract = CONTRACTS.get(feed)
    if contract is None:
        raise FileRejected("unknown_feed", f"no contract is registered for feed {feed!r}")
    if not isinstance(payload, dict):
        raise FileRejected(
            "contract_violation",
            f"{feed} contract {contract.version}: top level must be a JSON object, "
            f"got {type(payload).__name__}",
        )
    try:
        parsed = contract.model.model_validate(payload)
    except ValidationError as exc:
        problems = _summarize(exc, payload, contract)
        raise FileRejected("contract_violation", _format(contract, problems)) from exc

    declared = getattr(parsed, contract.date_field)
    if declared != file_date:
        raise FileRejected(
            "date_mismatch",
            f"{feed} contract {contract.version}: {contract.date_field} is {declared} "
            f"but the file is named for {file_date}",
        )
    records = payload[contract.records_field]
    assert isinstance(records, list)
    return [dict(r) for r in records]


def _format(contract: Contract, problems: list[str]) -> str:
    shown = problems[:MAX_PROBLEMS_SHOWN]
    extra = len(problems) - len(shown)
    tail = f"; and {extra} more problem(s)" if extra > 0 else ""
    return f"{contract.feed} contract {contract.version}: " + "; ".join(shown) + tail


def _summarize(exc: ValidationError, payload: dict[str, Any], contract: Contract) -> list[str]:
    """Collapse per-record errors into one line per distinct problem, with a count."""
    groups: dict[tuple[str, str], list[Any]] = defaultdict(list)
    for err in exc.errors():
        loc = err["loc"]
        if len(loc) >= 3 and loc[0] == contract.records_field and isinstance(loc[1], int):
            container, index, field = loc[0], loc[1], ".".join(str(p) for p in loc[2:])
            groups[(container, f"{err['type']}|{field}|{err['msg']}")].append(index)
        else:
            where = ".".join(str(p) for p in loc) or "file"
            groups[("file", f"{err['type']}|{where}|{err['msg']}")].append(None)

    problems: list[str] = []
    for (container, key), indexes in groups.items():
        kind, field, message = key.split("|", 2)
        if container == "file":
            problems.append(f"{field}: {message}")
            continue
        count = len(indexes)
        first = indexes[0]
        if kind == "missing":
            unexpected = _unexpected_fields(payload, contract, first)
            hint = f" (unexpected fields: {', '.join(unexpected)})" if unexpected else ""
            problems.append(
                f"{count} of {_record_count(payload, contract)} {container} missing required "
                f"field '{field}'{hint}, first at index {first}"
            )
        else:
            problems.append(
                f"{count} {container} with bad '{field}' ({message}), first at index {first}"
            )
    return problems


def _record_count(payload: dict[str, Any], contract: Contract) -> int:
    records = payload.get(contract.records_field)
    return len(records) if isinstance(records, list) else 0


def _unexpected_fields(payload: dict[str, Any], contract: Contract, index: int) -> list[str]:
    records = payload.get(contract.records_field)
    if (
        not isinstance(records, list)
        or index >= len(records)
        or not isinstance(records[index], dict)
    ):
        return []
    known = _known_fields(contract)
    return sorted(set(records[index]) - known)


def _known_fields(contract: Contract) -> set[str]:
    annotation = contract.model.model_fields[contract.records_field].annotation
    (item_model,) = get_args(annotation)  # list[ItemModel] -> ItemModel
    return set(item_model.model_fields)
