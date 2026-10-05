select
    o.order_id,
    (line.value ->> 'line_no')::integer as line_no,
    line.value ->> 'vendor' as vendor,
    line.value ->> 'sku' as sku_as_entered,
    (line.value ->> 'qty')::integer as qty,
    (line.value ->> 'unit_price')::numeric(12, 2) as unit_price
from {{ ref('stg_orders') }} as o
cross join lateral jsonb_array_elements(o.lines) as line (value)
