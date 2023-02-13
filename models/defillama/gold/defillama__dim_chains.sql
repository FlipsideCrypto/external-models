{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['defillama']
) }}

SELECT
    chain_id,
    chain,
    token_symbol
FROM {{ ref('bronze__defillama_chains') }}