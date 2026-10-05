"""SKU normalization. The dbt macro `normalize_sku` implements the same rules in SQL.

Both are checked against dbt/seeds/sku_normalization_cases.csv, so a change to one that is not
mirrored in the other fails a test. Rules, in order:
  1. lowercase
  2. drop everything that is not a letter or digit
  3. drop the vendor prefix (vendor_a: "ac", vendor_b: "b", vendor_c: "c")
  4. drop leading zeros
"""

from __future__ import annotations

import re

VENDOR_PREFIXES: dict[str, str] = {"vendor_a": "ac", "vendor_b": "b", "vendor_c": "c"}

_NON_ALNUM = re.compile(r"[^a-z0-9]")


def normalize_sku(raw: str, vendor: str) -> str:
    """Return the match key for a SKU as typed. Unknown vendors get no prefix stripped."""
    cleaned = _NON_ALNUM.sub("", raw.lower())
    prefix = VENDOR_PREFIXES.get(vendor, "")
    if prefix and cleaned.startswith(prefix):
        cleaned = cleaned[len(prefix) :]
    return cleaned.lstrip("0")


def product_key(vendor: str, raw_sku: str) -> str:
    return f"{vendor}:{normalize_sku(raw_sku, vendor)}"
