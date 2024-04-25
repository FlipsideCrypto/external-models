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
    chain_id,
    extensions AS token_extensions,
    provider,
    list_metadata,
    tokenlists_verified_tokens_id AS dim_verified_tokens_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__tokenlists_verified_tokens') }}
