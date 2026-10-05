{# A line without a product must say why: its match_method has to be 'unmatched'. #}
{% test unmatched_lines_are_labelled(model) %}
select *
from {{ model }}
where product_key is null
  and match_method is distinct from 'unmatched'
{% endtest %}
