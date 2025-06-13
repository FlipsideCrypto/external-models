{% macro sp_refresh_external_tables_aptos() %}
{% set sql %}
create or replace procedure bronze.sp_refresh_external_tables_aptos()
returns boolean
language sql
execute as caller
as
$$
    begin 
        alter external table EXTERNAL.BRONZE.aptos_shinam refresh; 
        return TRUE;
    end;
$${% endset %}
{% do run_query(sql) %}
{% endmacro %}