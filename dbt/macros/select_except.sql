{% macro select_except(relation, exclude_cols) %}
    {% set cols = adapter.get_columns_in_relation(relation) %}
    {{ cols | map(attribute='name') | reject('in', exclude_cols) | join(', ') }}
{% endmacro %}
