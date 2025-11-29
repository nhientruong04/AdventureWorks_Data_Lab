{% macro join_snapshots(left_snapshot, right_snapshot, left_key, right_key,
left_valid_from, left_valid_to, right_valid_from, right_valid_to, right_attributes) %}

{% set left_name = left_snapshot if left_snapshot is string else left_snapshot.name %}
{% set right_name = right_snapshot if right_snapshot is string else right_snapshot.name %}

SELECT
    {{left_name}}.* EXCEPT(valid_from, valid_to),
    {% for col in right_attributes %}
        {{right_name}}.{{col}} AS {{right_name}}__{{col}},
    {% endfor %}
    GREATEST({{left_name}}.{{left_valid_from}}, COALESCE({{right_name}}.{{right_valid_from}},
            {{left_name}}.{{left_valid_from}})) AS valid_from,
    LEAST({{left_name}}.{{left_valid_to}}, COALESCE({{right_name}}.{{right_valid_to}},
        {{left_name}}.{{left_valid_to}})) AS valid_to
FROM {{left_snapshot}} AS {{left_name}}
LEFT JOIN {{right_snapshot}} AS {{right_name}}
    ON {{left_name}}.{{left_key}} = {{right_name}}.{{right_key}}
    AND {{right_name}}.{{right_valid_to}} >= {{left_name}}.{{left_valid_from}}
    AND {{right_name}}.{{right_valid_from}} <= {{left_name}}.{{left_valid_to}}
{% endmacro %}
