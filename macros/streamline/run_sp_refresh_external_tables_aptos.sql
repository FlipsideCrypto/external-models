{% macro run_sp_refresh_external_tables_aptos() %}
{% set sql %}
call bronze.sp_refresh_external_tables_aptos();
{% endset %}

{% do run_query(sql) %}
{% endmacro %}