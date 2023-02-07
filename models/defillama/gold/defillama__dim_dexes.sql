{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['defillama']
) }}

SELECT
    dex_id,
    dex,
    category,
    chains 
FROM {{ ref('bronze__defillama_dexes') }}