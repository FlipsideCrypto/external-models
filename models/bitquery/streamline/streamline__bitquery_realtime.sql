{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"bitquery",
        "sql_limit" :"100",
        "producer_batch_size" :"100",
        "worker_batch_size" :"100",
        "async_concurrent_requests": "10",
        "sql_source" :"{{this.identifier}}",
        "order_by_column": "date_day" }
    ),
    tags = ['streamline_realtime']
) }}

WITH metrics AS (

    SELECT
        date_day,
        blockchain,
        metric,
        query_text,
        variables
    FROM
        {{ ref("streamline__bitquery_metrics") }} A
        LEFT JOIN {{ ref("streamline__bitquery_complete") }}
        b USING (
            blockchain,
            metric,
            date_day
        )
    WHERE
        b._invocation_id IS NULL
)
SELECT
    TO_NUMBER(to_char(date_day, 'YYYYMMDD')) AS date_day,
    blockchain,
    metric,
    TO_NUMBER(to_char(SYSDATE() :: DATE, 'YYYYMMDD')) AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        'https://graphql.bitquery.io',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'Authorization',
            'Bearer {Authentication}'
        ),
        OBJECT_CONSTRUCT(
            'query',
            query_text,
            'variables',
            variables
        ),
        'Vault/prod/external/bitquery'
    ) AS request
FROM
    metrics
