{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['defillama']
) }}

SELECT
    TIMESTAMP :: DATE AS DATE,
    bridge_id,
    bridge,
    deposit_txs,
    deposit_usd,
    withdraw_txs,
    withdraw_usd
FROM
    {{ ref('silver__defillama_bridge_volume') }}
