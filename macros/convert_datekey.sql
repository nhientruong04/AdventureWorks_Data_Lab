{% macro convert_datekey(date_col) %}
    CAST(FORMAT_DATE('%Y%m%d', DATE({{ date_col }})) as INT64)
{% endmacro %}
