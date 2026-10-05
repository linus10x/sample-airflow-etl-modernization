select
    match_method,
    count(*) as line_count,
    count(distinct (vendor, sku_as_entered)) as distinct_skus,
    round(100.0 * count(*) / sum(count(*)) over (), 1) as pct_of_lines
from {{ ref('fct_order_lines') }}
group by match_method
