{#
  Fails when a row that exists in the source model is missing from the tested model.
  This is the guard on the crosswalk: it may label a line "unmatched", it may not lose it.
#}
{% test no_dropped_lines(model, source_model, keys) %}
select s.*
from {{ source_model }} as s
left join {{ model }} as m
    on {% for k in keys %}m.{{ k }} = s.{{ k }}{% if not loop.last %} and {% endif %}{% endfor %}
where m.{{ keys[0] }} is null
{% endtest %}
