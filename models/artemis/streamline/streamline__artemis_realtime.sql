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

WITH complete_data AS (
 
    SELECT
        date_day,
        _invocation_id,
        MAX(date_day) OVER () AS max_complete_date
    FROM
        {{ ref("streamline__artemis_complete") }}
   
),
date_params AS (
    SELECT
        COALESCE(
            DATEADD(day, 1, (SELECT MAX(max_complete_date) FROM complete_data)),
            '2025-01-01'::DATE  -- Default backfill start date
        ) AS min_date,
        DATEADD(day, -2, SYSDATE()) AS max_date
    FROM
        complete_data
),
metrics AS (
    SELECT
        m.date_day,
        m.blockchain,
        m.metric,
        m.url,
        m.endpoint,
        TO_CHAR(p.min_date, 'YYYY-MM-DD') AS start_date,
        TO_CHAR(p.max_date, 'YYYY-MM-DD') AS end_date
    FROM
        {{ ref("streamline__artemis_metrics") }} m
        CROSS JOIN date_params p
        LEFT JOIN complete_data c
            ON m.date_day = c.date_day
    WHERE
        m.date_day between p.min_date and p.max_date
        AND c._invocation_id IS NULL
),
batch_data AS (
    SELECT 
        MIN(date_day) AS first_day,
        url,
        REPLACE(LISTAGG(DISTINCT endpoint, ',') WITHIN GROUP (ORDER BY endpoint), ',', '%2C') AS encoded_endpoints,
        REPLACE(LISTAGG(DISTINCT blockchain, ',') WITHIN GROUP (ORDER BY blockchain), ',', '%2C') AS encoded_ids,
        MIN(start_date) AS min_start_date,
        MAX(end_date) AS max_end_date
    FROM 
        metrics
    GROUP BY 
        2
)
SELECT
    TO_NUMBER(TO_CHAR(first_day, 'YYYYMMDD')) AS DATE_DAY,
    TO_NUMBER(TO_CHAR(SYSDATE(), 'YYYYMMDD')) AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        url || encoded_endpoints || '?APIKey={Authentication}' || '&symbols=' || encoded_ids || '&startDate=' || min_start_date || '&endDate=' || max_end_date, {}, {},
        'Vault/prod/external/artemis'
    ) AS request
FROM
    batch_data
