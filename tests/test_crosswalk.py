import csv
from pathlib import Path

import pytest

from pipeline.crosswalk import normalize_sku, product_key

CASES = list(
    csv.DictReader(
        (Path(__file__).parent.parent / "dbt" / "seeds" / "sku_normalization_cases.csv").open()
    )
)


@pytest.mark.parametrize("case", CASES, ids=[f"{c['vendor']}:{c['raw']}" for c in CASES])
def test_normalization_cases(case: dict[str, str]) -> None:
    assert normalize_sku(case["raw"], case["vendor"]) == case["expected"]


def test_unknown_vendor_keeps_letters_and_strips_only_zeros_and_separators() -> None:
    assert normalize_sku("X-00A1", "vendor_z") == "x00a1"


def test_product_key_joins_vendor_and_normalized_sku() -> None:
    assert product_key("vendor_c", "C0010023") == "vendor_c:10023"


def test_prefix_is_only_stripped_once() -> None:
    assert normalize_sku("AC-AC-77", "vendor_a") == "ac77"
