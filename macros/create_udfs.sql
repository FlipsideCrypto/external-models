{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {% set sql %}
        {{ create_udf_bulk_rest_api_v2() }};
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
