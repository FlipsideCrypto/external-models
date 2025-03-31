{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"artemis",
        "sql_limit" :"10",
        "producer_batch_size" :"10",
        "worker_batch_size" :"10",
        "async_concurrent_requests": "1",
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
        url,
        endpoint,
        to_char(
            date_day,
            'YYYY-MM-DD'
        ) AS query_date
    FROM
        {{ ref("streamline__artemis_metrics") }}
        LEFT JOIN {{ ref("streamline__oklink_complete") }}
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
        'GET',
        url || endpoint || '?APIKey={Authentication}' || '&artemisIds=' || blockchain || '&startDate=' || query_date || '&endDate=' || query_date,{},{},
        'Vault/prod/external/artemis'
    ) AS request
FROM
    metrics
