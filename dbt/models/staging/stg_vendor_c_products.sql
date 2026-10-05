select
    'vendor_c' as vendor,
    file_date as snapshot_date,
    payload ->> 'ProductCode' as vendor_sku,
    payload ->> 'Description' as product_name,
    (payload ->> 'Cost')::numeric(12, 2) as unit_cost,
    (payload ->> 'Inventory')::integer as stock_qty
from {{ ref('stg_loaded_records') }}
where feed = 'vendor_c'
