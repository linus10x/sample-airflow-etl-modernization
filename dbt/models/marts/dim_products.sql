select
    vendor || ':' || sku_norm as product_key,
    vendor,
    vendor_sku,
    sku_norm,
    product_name,
    unit_cost,
    stock_qty,
    last_seen_date,
    snapshot_count
from {{ ref('int_vendor_products_latest') }}
