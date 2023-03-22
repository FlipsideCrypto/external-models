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
    TIMESTAMP :: DATE AS DATE,
    bridge_id,
    bridge,
    bridge_name,
    deposit_txs,
    deposit_usd,
    withdraw_txs,
    withdraw_usd
FROM
    {{ ref('silver__defillama_bridge_volume') }}
