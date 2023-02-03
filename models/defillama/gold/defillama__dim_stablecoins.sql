{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['defillama']
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