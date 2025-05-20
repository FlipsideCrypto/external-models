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
    DATE,
    chain, 
    bridge_id,
    bridge,
    bridge_name,
    deposit_txs,
    deposit_usd,
    withdraw_txs,
    withdraw_usd,
    defillama_bridge_vol_by_chain_id as defillama_fact_bridge_volume_by_chain_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__defillama_bridge_volume_by_chain') }}