# Data sources

Every file this sample reads is generated. There is no real vendor, retailer, customer or
product data in this repository.

| Data | Origin | License | Seed |
|---|---|---|---|
| Vendor catalog snapshots (3 vendors, 30 days) | `scripts/generate.py` | generated, no third-party content | 42 |
| Orders export (5,000 orders, 30 days) | `scripts/generate.py` | generated, no third-party content | 42 |
| `dbt/seeds/sku_overrides.csv` | written by `scripts/generate.py` | generated | 42 |
| `dbt/seeds/sku_normalization_cases.csv` | written by hand for the normalizer tests | MIT OR Apache-2.0 | n/a |

Product names are an adjective, a noun and a number from short word lists. Customer references
are random numbers. The generator is deterministic: the same seed writes the same bytes, and a
test checks that.

Generated files go to `data/generated/` and are not committed.
