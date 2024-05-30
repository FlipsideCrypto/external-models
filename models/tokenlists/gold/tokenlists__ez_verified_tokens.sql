{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['tokenlists'],
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'TOKEN LISTS' } } }
) }}

SELECT
    LOWER(address) AS token_address,
    NAME,
    symbol,
    decimals,
    t.chain_id,
    chain AS chain_name,
    chain_symbol,
    extensions AS token_extensions,
    provider,
    list_metadata,
    tokenlists_verified_tokens_id AS ez_verified_tokens_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__tokenlists_verified_tokens') }} t 
LEFT JOIN {{ ref('defillama__dim_chains')}} d 
    ON t.chain_id = d.chain_id
