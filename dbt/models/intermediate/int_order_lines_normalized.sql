select
    l.order_id,
    l.line_no,
    l.vendor,
    l.sku_as_entered,
    {{ normalize_sku('l.sku_as_entered', 'l.vendor') }} as sku_norm,
    l.qty,
    l.unit_price
from {{ ref('stg_order_lines') }} as l
