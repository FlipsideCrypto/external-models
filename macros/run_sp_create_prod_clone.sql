{% macro run_sp_create_prod_clone() %}
    {% set clone_query %}
    call external._internal.create_prod_clone(
        'external',
        'external_dev',
        'external_dev_owner'
    );
{% endset %}
    {% do run_query(clone_query) %}
{% endmacro %}
