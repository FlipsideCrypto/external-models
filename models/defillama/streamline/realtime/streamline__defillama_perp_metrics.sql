{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"defillama_perp_metrics",
        "sql_limit" :"10000",
        "producer_batch_size" :"10",
        "worker_batch_size" :"1",
        "async_concurrent_requests" :"1",
        "sql_source" :"{{this.identifier}}"
        }
    ),
    tags = ['defillama_streamline']
) }}

WITH perps as (

    select 
        protocol_slug,
        protocol_id
    from {{ ref('bronze__defillama_perps') }}
)
SELECT
    protocol_slug,
    protocol_id,
    date_part('epoch_second', sysdate()) as run_timestamp,
    date_part('epoch_second', sysdate()::DATE) AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        'https://pro-api.llama.fi/{api_key}/api/summary/derivatives/'|| protocol_slug || '?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=false',
        OBJECT_CONSTRUCT(
            'Content-Type', 'text/plain',
            'Accept', 'text/plain',
            'fsc-quantum-state', 'streamline'
        ),
        {},
        'Vault/prod/external/defillama'
    ) AS request
FROM
    perps
where protocol_slug is not null
limit 10000