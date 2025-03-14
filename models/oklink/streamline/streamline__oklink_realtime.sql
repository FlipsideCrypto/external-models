{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table": "oklink",
        "sql_limit": "1",
        "producer_batch_size": "1",
        "worker_batch_size": "1",
        "async_concurrent_requests": "1",
        "sql_source": "{{this.identifier}}",
        "order_by_column": "date_day" }
    ),
    tags = ['streamline_realtime']
) }}

WITH metrics AS (

    SELECT
        date_day,
        blockchain,
        metric,
        endpoint || '?chainShortName=' || chain_short_name || CASE
            WHEN metric = 'address' THEN ''
            WHEN metric = 'stats' THEN '&startTime=' || xtime || '&endTime=' || xtime || '&limit=' || 1 || '&page=' || 1
        END AS full_endpoint
    FROM
        {{ ref("streamline__oklink_metrics") }} A
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
        'GET',
        'https://www.oklink.com/api/v5/explorer/' || full_endpoint,
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'Ok-Access-Key',
            '{Authentication}'
        ),{},
        'Vault/prod/external/oklink'
    ) AS request
FROM
    metrics
