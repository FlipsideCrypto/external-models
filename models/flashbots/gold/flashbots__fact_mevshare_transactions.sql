{{ config(
    materialized = "view",
    tags = ['flashbots'],
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'flashbots'
            }
        }
    }
) }}

SELECT
    block_number,
    block_time,
    block_hash,
    extra_data,
    fee_recipient_address,
    bundle_id,
    user_tx_hash,
    user_tx_from,
    user_tx_to,
    backrun_tx_hash,
    backrun_tx_from,
    backrun_tx_to,
    refund_tx_hash,
    refund_from,
    refund_to,
    refund_value_eth,
    is_mevshare
FROM
    {{ ref('silver__flashbots_mev_txs') }}
