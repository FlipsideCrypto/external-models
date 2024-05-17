{{ config(
    materialized = "table",
    unique_key = "layerzero_txs_snapshot1_id",
    cluster_by = "source_timestamp_utc::DATE",
    tags = ['layerzero']
) }}

WITH base AS (

    SELECT
        IFF(
            source_chain = '""',
            NULL,
            REGEXP_REPLACE(
                source_chain,
                '^"|\\s*"$',
                ''
            )
        ) AS source_chain,
        IFF(
            source_transaction_hash = '""',
            NULL,
            REGEXP_REPLACE(
                source_transaction_hash,
                '^"|\\s*"$',
                ''
            )
        ) AS source_transaction_hash,
        IFF(
            source_contract = '""',
            NULL,
            REGEXP_REPLACE(
                source_contract,
                '^"|\\s*"$',
                ''
            )
        ) AS source_contract,
        IFF(
            source_chain = '""',
            NULL,
            REGEXP_REPLACE(
                destination_chain,
                '^"|\\s*"$',
                ''
            )
        ) AS destination_chain,
        IFF(
            destination_transaction_hash = '""',
            NULL,
            REGEXP_REPLACE(
                destination_transaction_hash,
                '^"|\\s*"$',
                ''
            )
        ) AS destination_transaction_hash,
        IFF(
            destination_contract = '""',
            NULL,
            REGEXP_REPLACE(
                destination_contract,
                '^"|\\s*"$',
                ''
            )
        ) AS destination_contract,
        IFF(
            sender_wallet = '""',
            NULL,
            REGEXP_REPLACE(
                sender_wallet,
                '^"|\\s*"$',
                ''
            )
        ) AS sender_wallet,
        TO_TIMESTAMP_NTZ(
            IFF(
                source_timestamp_utc = '""',
                NULL,
                REGEXP_REPLACE(
                    source_timestamp_utc,
                    '^"|\\s*"$',
                    ''
                )
            )
        ) AS source_timestamp_utc,
        IFF(
            project = '""',
            NULL,
            REGEXP_REPLACE(
                project,
                '^"|\\s*"$',
                ''
            )
        ) AS project,
        IFF(
            native_drop_usd = '""',
            NULL,
            REGEXP_REPLACE(
                native_drop_usd,
                '^"|\\s*"$',
                ''
            )
        ) AS native_drop_usd,
        IFF(
            stargate_swap_usd = '""',
            NULL,
            REGEXP_REPLACE(
                stargate_swap_usd,
                '^"|\\s*"$',
                ''
            )
        ) AS stargate_swap_usd,
        ROW_NUMBER() over (
            PARTITION BY source_transaction_hash
            ORDER BY
                source_chain ASC
        ) AS tx_rn,
        snapshot_version,
        _inserted_timestamp
    FROM
        {{ ref('bronze__layerzero_txs_snapshot1') }}
)
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
    tx_rn,
    snapshot_version,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['source_transaction_hash', 'tx_rn']
    ) }} AS layerzero_txs_snapshot1_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    base
