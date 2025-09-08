{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"defillama_stablecoin_metrics",
        "sql_limit" :"10000",
        "producer_batch_size" :"10",
        "worker_batch_size" :"1",
        "async_concurrent_requests" :"1",
        "sql_source" :"{{this.identifier}}",
        "exploded_key": tojson(['data'])
        }
    ),
    tags = ['defillama_streamline']
) }}

WITH stablecoins as (

    select 
    stablecoin_id,
    stablecoin,
    symbol,
    peg_type,
    peg_mechanism,
    price_source,
    value::string as chain
    from {{ ref('bronze__defillama_stablecoins') }},
    lateral flatten (input => chains)
)
SELECT
    chain,
    stablecoin_id,
    date_part('epoch_second', sysdate()) as run_timestamp,
    date_part('epoch_second', sysdate()::DATE) AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        'https://pro-api.llama.fi/{api_key}/stablecoins/stablecoincharts/' || chain || '?stablecoin=' || stablecoin_id,
        OBJECT_CONSTRUCT(
            'Content-Type', 'text/plain',
            'Accept', 'text/plain',
            'fsc-quantum-state', 'streamline'
        ),
        {},
        'Vault/prod/external/defillama'
    ) AS request
FROM
    stablecoins
where chain is not null and stablecoin_id is not null

order by chain

limit 10000