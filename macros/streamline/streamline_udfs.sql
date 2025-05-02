{% macro create_udf_bulk_rest_api_v2() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api_v2(
        json OBJECT
    ) returns ARRAY {% if target.database == 'EXTERNAL' -%}
        api_integration = aws_external_api_prod_v2 AS 'https://zv7a5qfhv9.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_rest_api'
    {% else %}
        api_integration = aws_external_api_stg_v2 AS 'https://qoupd0givh.execute-api.us-east-1.amazonaws.com/stg/udf_bulk_rest_api'
    {%- endif %}
{% endmacro %}
