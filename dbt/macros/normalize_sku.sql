{#
  Match key for a SKU as typed. Same rules as pipeline/crosswalk.py, checked against the
  shared cases in seeds/sku_normalization_cases.csv:
    1. lowercase  2. drop non letters and digits  3. drop the vendor prefix  4. drop leading zeros
#}
{% macro normalize_sku(sku, vendor) -%}
ltrim(
    regexp_replace(
        regexp_replace(lower({{ sku }}), '[^a-z0-9]', '', 'g'),
        case {{ vendor }}
            when 'vendor_a' then '^ac'
            when 'vendor_b' then '^b'
            when 'vendor_c' then '^c'
            else '^$'
        end,
        ''
    ),
    '0'
)
{%- endmacro %}
