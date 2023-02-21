{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['defillama'],
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'DEFILLAMA'
            }
        }
    }
) }}

SELECT
    stablecoin_id,
    stablecoin,
    symbol,
    peg_type,
    peg_mechanism,
    price_source,
    chains
FROM {{ ref('bronze__defillama_stablecoins') }}