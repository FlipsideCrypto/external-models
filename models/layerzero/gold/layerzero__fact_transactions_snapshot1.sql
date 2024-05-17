{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['layerzero'],
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'layerzero' } } }
) }}


SELECT
    source_chain,
    source_transaction_hash,
    source_contract,
    destination_chain,
    destination_transaction_hash,
    destination_contract,
    sender_wallet,
    source_timestamp_utc,
    project,
    native_drop_usd,
    stargate_swap_usd,
    layerzero_txs_snapshot1_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__layerzero_txs_snapshot1') }}
