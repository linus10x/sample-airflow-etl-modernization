select
    payload ->> 'order_id' as order_id,
    (payload ->> 'placed_at')::timestamp as placed_at,
    file_date as export_date,
    payload ->> 'customer_ref' as customer_ref,
    payload -> 'lines' as lines
from {{ ref('stg_loaded_records') }}
where feed = 'orders'
