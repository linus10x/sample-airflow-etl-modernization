select
    'vendor_b' as vendor,
    file_date as snapshot_date,
    payload ->> 'item_code' as vendor_sku,
    payload ->> 'title' as product_name,
    (payload ->> 'unit_cost')::numeric(12, 2) as unit_cost,
    (payload ->> 'stock')::integer as stock_qty
from {{ ref('stg_loaded_records') }}
where feed = 'vendor_b'
