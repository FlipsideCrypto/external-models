{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['stale'],
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'DEFILLAMA'
            }
        }
    }
) }}

SELECT
    date,
    stablecoin_id,
    stablecoin,
    symbol,
    chain,
    total_bridged_usd,
    total_circulating,
    total_circulating_usd,
    defillama_usdc_usdt_supply_id as defillama_fact_usdt_usdc_supply_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__defillama_usdt_usdc_supply') }} f
