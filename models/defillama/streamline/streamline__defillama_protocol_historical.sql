{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"defillama_protocol_historical",
        "sql_limit" :"200",
        "producer_batch_size" :"200",
        "worker_batch_size" :"200",
        "async_concurrent_requests" :"1",
        "sql_source" :"{{this.identifier}}" }
    ),
    tags = ['defillama_history']
) }}

WITH base AS (

    SELECT
        protocol_slug,
        protocol_id
    FROM
        {{ ref('defillama__dim_protocols') }}
    WHERE
        protocol_id NOT IN (
            SELECT
                protocol_id
            FROM
                {{ ref('streamline__defillama_protocol_historical_complete') }}
            WHERE
                protocol_id IS NOT NULL
        )
        AND protocol_id IN (
            SELECT
                PROTOCOL_ID
            FROM
                {{ ref('bronze__defillama_protocol_tvl_historical_response_sizes') }}
            WHERE
                size_mb < 15
            AND
                status_code = 200
        )
    ORDER BY
        protocol_id ASC
    LIMIT 200
)
SELECT
    protocol_id,
    date_part('epoch_second', sysdate()::DATE) AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        'https://pro-api.llama.fi/{api_key}/api/protocol/' || protocol_slug,
        OBJECT_CONSTRUCT(
            'Content-Type', 'text/plain',
            'Accept', 'text/plain'
        ),
        {},
        'Vault/prod/external/defillama'
    ) AS request
FROM
    base
ORDER BY
    protocol_id ASC
