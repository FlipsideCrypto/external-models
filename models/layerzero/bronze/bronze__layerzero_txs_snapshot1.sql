{{ config (
    materialized = 'view'
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
    SYSDATE() AS _inserted_timestamp,
    'snapshot_one' AS snapshot_version
FROM
    {{ source(
        "layerzero",
        "transactions"
    ) }}
