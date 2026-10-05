{#
  One row per SKU string that appears on an order line, tied to a product when possible.
  Priority: a hand-made override first, then an exact match on the normalized key.
  Anything else is kept and labelled "unmatched" so it shows up in the report.
#}
with entered as (
    select distinct vendor, sku_as_entered, sku_norm
    from {{ ref('int_order_lines_normalized') }}
)

select
    e.vendor,
    e.sku_as_entered,
    e.sku_norm,
    coalesce(o.product_key, p.product_key) as product_key,
    case
        when o.product_key is not null then 'manual_override'
        when p.product_key is not null then 'exact_normalized'
        else 'unmatched'
    end as match_method
from entered as e
left join {{ ref('sku_overrides') }} as o
    on o.vendor = e.vendor and o.sku_norm_entered = e.sku_norm
left join {{ ref('dim_products') }} as p
    on p.vendor = e.vendor and p.sku_norm = e.sku_norm
