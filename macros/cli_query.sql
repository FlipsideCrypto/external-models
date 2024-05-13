-- macros/print_query_results.sql

{% macro execute_and_print_query() %}
    {% set results = run_query("select distinct(model) from bronze.defillama_historical_backfill_list") %}
    
    {% if execute %}
        {% for row in results %}
            {{ log(row, info=True) }}
        {% endfor %}
    {% endif %}
{% endmacro %}
