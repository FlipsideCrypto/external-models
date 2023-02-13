{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['defillama']
) }}

SELECT
    TIMESTAMP :: DATE AS DATE,
    chain,id,
    chain,
    tvl_usd    
FROM
    {{ ref('silver__defillama_chains_tvl') }}
