{# The SQL macro must agree with every case that the Python normalizer is tested against. #}
select vendor, raw, expected, {{ normalize_sku('raw', 'vendor') }} as actual
from {{ ref('sku_normalization_cases') }}
where {{ normalize_sku('raw', 'vendor') }} is distinct from expected::text
