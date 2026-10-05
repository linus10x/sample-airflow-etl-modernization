{# Every order line stays, matched or not. A left join is the point: nothing is dropped. #}
select
    l.order_id,
    l.line_no,
    o.placed_at::date as order_date,
    l.vendor,
    l.sku_as_entered,
    l.sku_norm,
    c.product_key,
    c.match_method,
    l.qty,
    l.unit_price,
    (l.qty * l.unit_price)::numeric(14, 2) as line_total,
    p.unit_cost as latest_unit_cost,
    case
        when p.product_key is not null then ((l.unit_price - p.unit_cost) * l.qty)::numeric(14, 2)
    end as line_margin
from {{ ref('int_order_lines_normalized') }} as l
inner join {{ ref('stg_orders') }} as o on o.order_id = l.order_id
left join {{ ref('crosswalk_sku') }} as c
    on c.vendor = l.vendor and c.sku_as_entered = l.sku_as_entered
left join {{ ref('dim_products') }} as p on p.product_key = c.product_key
