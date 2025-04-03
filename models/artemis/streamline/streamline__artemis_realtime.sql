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
        endpoint
    FROM
        {{ ref("streamline__artemis_metrics") }}
        LEFT JOIN {{ ref("streamline__artemis_complete") }}
        b USING (
            blockchain,
            metric,
            date_day
        )
    WHERE
        b._invocation_id IS NULL
),
batch_data AS (
    SELECT 
        date_day,
        url,
        REPLACE(LISTAGG(DISTINCT endpoint, ',') WITHIN GROUP (ORDER BY endpoint), ',', '%2C') AS encoded_endpoints,
        REPLACE(LISTAGG(DISTINCT blockchain, ',') WITHIN GROUP (ORDER BY blockchain), ',', '%2C') AS encoded_ids,
    FROM 
        metrics
    GROUP BY 
        1,2
)
SELECT
    TO_NUMBER(to_char(date_day, 'YYYYMMDD')) AS date_day,
    TO_NUMBER(to_char(SYSDATE() :: DATE, 'YYYYMMDD')) AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        url || encoded_endpoints || '?APIKey={Authentication}' || '&artemisIds=' || encoded_ids || '&startDate=' || date_day || '&endDate=' || date_day, {}, {},
        'Vault/prod/external/artemis'
    ) AS request
FROM
    batch_data
