{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"defillama_protocol_historical",
        "sql_limit" :"10",
        "producer_batch_size" :"1",
        "worker_batch_size" :"1",
        "async_concurrent_requests" :"1",
        "sql_source" :"{{this.identifier}}" }
    ),
    tags = ['streamline_defillama']
) }}

WITH base AS (

    SELECT
        protocol_slug,
        protocol_id,
        row_num
    FROM
        {{ ref('bronze__defillama_protocols') }}
    WHERE
        protocol_id NOT IN (
            SELECT
                protocol_id
            FROM
                {{ ref('streamline__defillama_protocol_historical_complete') }}
            WHERE
                protocol_id IS NOT NULL
        )
    ORDER BY
        row_num ASC
    LIMIT 5
)
SELECT
    protocol_id,
    ROUND(
        protocol_id,
        -1
    ) AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        'https://pro-api.llama.fi/{api_key}/api/hourly/' || protocol_slug,{},{},
        'Vault/prod/external/defillama'
    ) AS request
FROM
    base
ORDER BY
    row_num ASC
