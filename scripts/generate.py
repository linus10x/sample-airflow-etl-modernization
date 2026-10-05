#!/usr/bin/env python3
"""Generate the synthetic dataset: vendor catalog snapshots and internal order exports.

Everything is derived from one seed, so a run with the same seed writes the same bytes.

  3 vendors, each with its own SKU format and field names
  800 products split across the vendors
  30 daily files per feed starting 2026-03-01
  5,000 orders in the internal orders export
  1 drift file: on day 5 vendor_c sends "UnitCost" where its contract says "Cost"

Order lines carry the SKU as someone typed it, so the crosswalk has real work to do: wrong case,
dropped prefixes, spaces for dashes, a handful of retired SKUs covered by the manual override
seed, and a few typos that stay unmatched on purpose.

Usage: python scripts/generate.py --seed 42 [--out data/generated]
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.crosswalk import normalize_sku, product_key  # noqa: E402

START = date(2026, 3, 1)
DAYS = 30
N_PRODUCTS = 800
N_ORDERS = 5000
DRIFT_DAY = 5
VENDOR_SPLIT = {"vendor_a": 300, "vendor_b": 260, "vendor_c": 240}

ADJECTIVES = [
    "Trail",
    "Harbor",
    "Cedar",
    "Summit",
    "Meadow",
    "Copper",
    "Linen",
    "Alpine",
    "Coastal",
    "Ember",
    "Willow",
    "Granite",
    "Maple",
    "Juniper",
    "Slate",
    "Birch",
    "Canyon",
    "Prairie",
]
NOUNS = [
    "Bottle",
    "Backpack",
    "Blanket",
    "Lantern",
    "Kettle",
    "Journal",
    "Towel",
    "Mug",
    "Pillow",
    "Tote",
    "Mat",
    "Cooler",
    "Candle",
    "Apron",
    "Bowl",
    "Tray",
    "Rug",
    "Basket",
    "Jar",
    "Throw",
]


def money(value: float) -> float:
    return round(value + 1e-9, 2)


def vendor_sku(vendor: str, n: int, rng: random.Random) -> str:
    if vendor == "vendor_a":
        return f"AC-{n:05d}"
    if vendor == "vendor_b":
        suffix = f"_{rng.choice('xyz')}" if rng.random() < 0.3 else ""
        return f"b_{n:05d}{suffix}"
    return f"C{n:07d}"


def build_products(rng: random.Random) -> list[dict[str, Any]]:
    vendors = [v for v, count in VENDOR_SPLIT.items() for _ in range(count)]
    assert len(vendors) == N_PRODUCTS
    rng.shuffle(vendors)
    products = []
    for i, vendor in enumerate(vendors):
        n = 10000 + i
        cost = money(rng.uniform(2.0, 80.0))
        products.append(
            {
                "vendor": vendor,
                "sku": vendor_sku(vendor, n, rng),
                "name": f"{rng.choice(ADJECTIVES)} {rng.choice(NOUNS)} {n}",
                "cost": cost,
                "price": money(cost * rng.uniform(1.4, 2.2)),
                "stock": rng.randint(0, 400),
            }
        )
    return products


def snapshot_for_day(
    vendor: str, products: list[dict[str, Any]], day_index: int, rng: random.Random
) -> list[dict[str, Any]]:
    """Cost and stock wander a little from day to day for a share of the items."""
    items = []
    for p in products:
        if rng.random() < 0.10:
            p["cost"] = money(max(0.5, p["cost"] * rng.uniform(0.95, 1.05)))
        p["stock"] = max(0, p["stock"] + rng.randint(-15, 25))
        items.append(dict(p))
    return items


def vendor_file(vendor: str, day: date, items: list[dict[str, Any]]) -> dict[str, Any]:
    if vendor == "vendor_a":
        return {
            "vendor": "vendor_a",
            "snapshot_date": day.isoformat(),
            "items": [
                {
                    "sku": i["sku"],
                    "name": i["name"],
                    "cost_price": i["cost"],
                    "qty_on_hand": i["stock"],
                }
                for i in items
            ],
        }
    if vendor == "vendor_b":
        return {
            "supplier": "vendor_b",
            "as_of": day.isoformat(),
            "products": [
                {
                    "item_code": i["sku"],
                    "title": i["name"],
                    "unit_cost": f"{i['cost']:.2f}",
                    "stock": i["stock"],
                }
                for i in items
            ],
        }
    cost_field = "UnitCost" if (day - START).days + 1 == DRIFT_DAY else "Cost"
    return {
        "feed": "vendor_c",
        "date": day.isoformat(),
        "rows": [
            {
                "ProductCode": i["sku"],
                "Description": i["name"],
                cost_field: i["cost"],
                "Inventory": i["stock"],
            }
            for i in items
        ],
    }


def typed_sku(p: dict[str, Any], rng: random.Random) -> str:
    """The SKU as a person might have typed it into the order system."""
    sku: str = p["sku"]
    roll = rng.random()
    if roll < 0.55:
        return sku
    if roll < 0.70:
        return sku.lower()
    if roll < 0.85:
        digits = "".join(ch for ch in sku if ch.isdigit())
        return digits.lstrip("0") or digits
    return sku.replace("-", " ").replace("_", " ")


def typo_sku(p: dict[str, Any], known: set[tuple[str, str]], rng: random.Random) -> str:
    """A transposed-digit SKU that does not collide with a real product."""
    sku: str = p["sku"]
    digit_positions = [i for i, ch in enumerate(sku) if ch.isdigit()]
    for _ in range(20):
        a, b = rng.sample(digit_positions, 2)
        chars = list(sku)
        chars[a], chars[b] = chars[b], chars[a]
        candidate = "".join(chars)
        if (p["vendor"], normalize_sku(candidate, p["vendor"])) not in known and candidate != sku:
            return candidate
    return sku + "9"


def build_orders(
    products: list[dict[str, Any]], rng: random.Random
) -> tuple[dict[int, list[dict[str, Any]]], list[dict[str, str]]]:
    known = {(p["vendor"], normalize_sku(p["sku"], p["vendor"])) for p in products}
    weights = [1.0 / (1 + i % 40) for i in range(len(products))]
    retired = rng.sample(products, 12)
    aliases = {p["sku"]: f"LEG-{1000 + i}" for i, p in enumerate(retired)}
    overrides = [
        {
            "vendor": p["vendor"],
            "sku_norm_entered": normalize_sku(aliases[p["sku"]], p["vendor"]),
            "product_key": product_key(p["vendor"], p["sku"]),
            "note": "retired legacy SKU, mapped by hand",
        }
        for p in retired
    ]

    per_day = [0] * DAYS
    for _ in range(N_ORDERS):
        per_day[min(DAYS - 1, int(rng.triangular(0, DAYS, DAYS * 0.6)))] += 1

    by_day: dict[int, list[dict[str, Any]]] = {}
    seq = 0
    for day_index in range(DAYS):
        day = START + timedelta(days=day_index)
        orders = []
        for _ in range(per_day[day_index]):
            seq += 1
            placed = datetime(day.year, day.month, day.day, rng.randint(0, 23), rng.randint(0, 59))
            lines = []
            for line_no in range(1, rng.randint(1, 4) + 1):
                p = rng.choices(products, weights=weights)[0]
                roll = rng.random()
                if p["sku"] in aliases and roll < 0.6:
                    sku = aliases[p["sku"]]
                elif roll < 0.03:
                    sku = typo_sku(p, known, rng)
                else:
                    sku = typed_sku(p, rng)
                lines.append(
                    {
                        "line_no": line_no,
                        "vendor": p["vendor"],
                        "sku": sku,
                        "qty": rng.randint(1, 5),
                        "unit_price": p["price"],
                    }
                )
            orders.append(
                {
                    "order_id": f"O-{seq:06d}",
                    "placed_at": placed.isoformat(),
                    "customer_ref": f"CUST-{rng.randint(1, 1200):04d}",
                    "lines": lines,
                }
            )
        by_day[day_index] = orders
    return by_day, overrides


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", type=Path, default=ROOT / "data" / "generated")
    parser.add_argument(
        "--overrides-out", type=Path, default=ROOT / "dbt" / "seeds" / "sku_overrides.csv"
    )
    args = parser.parse_args()

    rng = random.Random(args.seed)
    products = build_products(rng)
    by_vendor = {v: [p for p in products if p["vendor"] == v] for v in VENDOR_SPLIT}

    files = 0
    for day_index in range(DAYS):
        day = START + timedelta(days=day_index)
        for vendor, items in by_vendor.items():
            snapshot = snapshot_for_day(vendor, items, day_index, rng)
            write_json(
                args.out / "incoming" / vendor / f"{day.isoformat()}.json",
                vendor_file(vendor, day, snapshot),
            )
            files += 1

    orders_by_day, overrides = build_orders(products, rng)
    n_orders = n_lines = 0
    for day_index, orders in orders_by_day.items():
        day = START + timedelta(days=day_index)
        if not orders:
            continue
        write_json(
            args.out / "incoming" / "orders" / f"{day.isoformat()}.json",
            {"export_date": day.isoformat(), "orders": orders},
        )
        files += 1
        n_orders += len(orders)
        n_lines += sum(len(o["lines"]) for o in orders)

    args.overrides_out.parent.mkdir(parents=True, exist_ok=True)
    with args.overrides_out.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["vendor", "sku_norm_entered", "product_key", "note"],
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(sorted(overrides, key=lambda r: (r["vendor"], r["sku_norm_entered"])))

    drift_day = START + timedelta(days=DRIFT_DAY - 1)
    facts = {
        "seed": args.seed,
        "start": START.isoformat(),
        "days": DAYS,
        "drift_file": f"incoming/vendor_c/{drift_day.isoformat()}.json",
        "products": len(products),
        "files": files,
        "orders": n_orders,
        "order_lines": n_lines,
        "vendor_products": {v: len(items) for v, items in by_vendor.items()},
    }
    (args.out / "FACTS.json").write_text(json.dumps(facts, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(facts))
    return 0


if __name__ == "__main__":
    sys.exit(main())
