#!/usr/bin/env python3
"""The "before": a cron-style script that reads vendor and order JSON and writes CSV files.

This is a stand-in for the aging job a client describes. It is kept on purpose, with its habits
intact, so the new pipeline can be compared against it:

  * a file it cannot read is skipped without a word
  * a missing field becomes 0 instead of an error
  * nothing records which file was already processed, so a re-run starts from scratch

Usage: python legacy/legacy_etl.py <source dir with incoming/> <output dir>
       python legacy/legacy_etl.py --s3 <output dir>      (reads the MinIO bucket)
"""

import csv
import json
import os
import sys
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path


def read_files(source):
    """Yield (feed, name, parsed json) for every incoming file. Unreadable files are skipped."""
    if source == "--s3":
        import boto3

        client = boto3.client(
            "s3",
            endpoint_url=os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000"),
            aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "demo-minio-user"),
            aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "demo-minio-pass"),
            region_name="us-east-1",
        )
        bucket = os.environ.get("S3_BUCKET", "vendor-drops")
        pages = client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="incoming/")
        for page in pages:
            for obj in page.get("Contents", []):
                _, feed, name = obj["Key"].split("/", 2)
                try:
                    yield (
                        feed,
                        name,
                        json.loads(client.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()),
                    )
                except Exception:
                    continue
    else:
        for path in sorted(Path(source, "incoming").rglob("*.json")):
            try:
                yield path.parent.name, path.name, json.loads(path.read_text())
            except Exception:
                continue


def money(value):
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def run(source, out_dir):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    orders, products = [], []

    for feed, name, doc in read_files(source):
        day = name.replace(".json", "")
        if feed == "orders":
            for o in doc.get("orders", []):
                total = sum(money(ln["qty"]) * money(ln["unit_price"]) for ln in o["lines"])
                orders.append((o["order_id"], day, len(o["lines"]), total))
        elif feed == "vendor_a":
            for i in doc.get("items", []):
                products.append(
                    (feed, day, i["sku"], i["name"], i.get("cost_price", 0), i["qty_on_hand"])
                )
        elif feed == "vendor_b":
            for i in doc.get("products", []):
                products.append(
                    (feed, day, i["item_code"], i["title"], i.get("unit_cost", 0), i["stock"])
                )
        elif feed == "vendor_c":
            for i in doc.get("rows", []):
                products.append(
                    (
                        feed,
                        day,
                        i["ProductCode"],
                        i["Description"],
                        i.get("Cost", 0),
                        i["Inventory"],
                    )
                )

    with open(out / "orders.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["order_id", "order_date", "line_count", "order_total"])
        w.writerows(sorted(orders))
    with open(out / "vendor_products.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["vendor", "snapshot_date", "sku", "name", "cost", "stock"])
        w.writerows(products)
    print(f"legacy run: {len(orders)} orders, {len(products)} product rows")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    run(sys.argv[1], sys.argv[2])
