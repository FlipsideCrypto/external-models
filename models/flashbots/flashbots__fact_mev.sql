{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
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
    block_number::integer as block_number,
    block_time,
    block_hash,
    extra_data,
    fee_recipient_address,
    user_tx_hash,
    user_tx_from,
    user_tx_to,
    backrun_tx_hash,
    backrun_tx_from,
    backrun_tx_to,
    refund_tx_hash,
    refund_from,
    refund_to,
    refund_value_eth
FROM 
    {{ source(
        'flashbots',
        'mev'
    ) }}