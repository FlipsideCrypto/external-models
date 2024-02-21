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
    tx_hash,
    from_address,
    to_address,
    public_mempool,
    created_at_block_number,
    included_block_number,
    tx_id,
    hints_selected,
    num_of_builders_shared,
    refund_percent
FROM 
    {{ ref('silver__flashbots_protect_txs') }}
