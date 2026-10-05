{# One row per vendor product: the newest snapshot that loaded, plus how often it was seen. #}
with normalized as (
    select
        vendor,
        vendor_sku,
        {{ normalize_sku('vendor_sku', 'vendor') }} as sku_norm,
        product_name,
        unit_cost,
        stock_qty,
        snapshot_date
    from {{ ref('stg_vendor_products') }}
),

ranked as (
    select
        *,
        row_number() over (partition by vendor, sku_norm order by snapshot_date desc) as recency,
        count(*) over (partition by vendor, sku_norm) as snapshot_count
    from normalized
)

select
    vendor,
    vendor_sku,
    sku_norm,
    product_name,
    unit_cost,
    stock_qty,
    snapshot_date as last_seen_date,
    snapshot_count
from ranked
where recency = 1
