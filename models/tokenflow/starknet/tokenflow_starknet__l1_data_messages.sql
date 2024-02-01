{{ config(
    materialized = 'view',
    tags = ['tokenflow'],
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'TOKENFLOW, STARKNET',
                'PURPOSE': 'L1'
            }
        }
    }
) }}

SELECT
    *
FROM
    {{ source(
        'starknet_snapshot',
        'l1_messages'
    ) }}
