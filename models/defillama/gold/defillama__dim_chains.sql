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
    chain,
    chain_symbol,
    token_name,
    token_decimals,
    token_symbol,
    chain_id,
    network_id,
    rpc,
    faucets,
    info_url,
    short_name,
    explorers
FROM 
    {{ ref('bronze__defillama_chainlist') }}