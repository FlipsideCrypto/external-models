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
    tx_hash,
    from_address,
    to_address,
    public_mempool,
    created_at_block_number,
    included_block_number,
    hints_selected,
    num_of_builders_shared,
    refund_percent
FROM 
    {{ source(
        'flashbots',
        'protect'
    ) }}