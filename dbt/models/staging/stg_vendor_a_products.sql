select
    'vendor_a' as vendor,
    file_date as snapshot_date,
    payload ->> 'sku' as vendor_sku,
    payload ->> 'name' as product_name,
    (payload ->> 'cost_price')::numeric(12, 2) as unit_cost,
    (payload ->> 'qty_on_hand')::integer as stock_qty
from {{ ref('stg_loaded_records') }}
where feed = 'vendor_a'
