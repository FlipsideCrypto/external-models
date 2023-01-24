{{ config(
    materialized = 'view',
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
        'tokenflow_starknet_decoded',
        'traces'
    ) }}
