{{ config(
    materialized = 'view',
    tags = ['tokenflow'],
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'TOKENFLOW, STARKNET',
                'PURPOSE': 'DECODED'
            }
        }
    }
) }}

SELECT
    *
FROM
    {{ source(
        'starknet_snapshot',
        'traces'
    ) }}
