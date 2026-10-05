select
    order_id,
    min(order_date) as order_date,
    count(*) as line_count,
    sum(line_total)::numeric(14, 2) as order_total,
    count(*) filter (where match_method <> 'unmatched') as matched_lines,
    count(*) filter (where match_method = 'unmatched') as unmatched_lines
from {{ ref('fct_order_lines') }}
group by order_id
