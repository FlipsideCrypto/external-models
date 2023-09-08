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
        'tokenflow_starknet_l1_data',
        'blocks'
    ) }}
