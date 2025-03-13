{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = { 
        "external_table": "oklink",
        "sql_limit": "1",
        "producer_batch_size": "1",
        "worker_batch_size": "1",
        "async_concurrent_requests": "1",
        "sql_source": "{{this.identifier}}",
        "order_by_column": "date_day", 
        "request_delay_ms": "1000"}
    ),
    tags = ['streamline_realtime']
) }}

WITH metrics AS (
       SELECT
        date_day,
        blockchain,
        metric,
        CASE 
            WHEN metric = 'address_count' THEN
                endpoint || '?chainShortName=' || variables:chainShortName
            WHEN metric = 'blockchain_stats' THEN
                endpoint || '?chainShortName=' || variables:chainShortName ||
                '&startTime=' || variables:startTime ||
                '&endTime=' || variables:endTime ||
                '&limit=' || variables:limit ||
                '&page=' || variables:page
        END AS full_endpoint
    FROM
        {{ ref("streamline__oklink_metrics") }}
)
SELECT
    TO_NUMBER(to_char(date_day, 'YYYYMMDD')) AS date_day,
    blockchain,
    metric,
    TO_NUMBER(to_char(SYSDATE()::DATE, 'YYYYMMDD')) AS partition_key,
    livequery.live.udf_api(
        'GET',
        full_endpoint,
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'Ok-Access-Key', '{Authentication}'
        ),
        NULL,
        'Vault/prod/oklink'
    ) AS request
FROM
    metrics