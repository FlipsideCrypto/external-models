{{ config(
    materialized = 'view',
    tags = ['tokenflow'],
    persist_docs ={ "relation": true,
    "columns": true },
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
        'events'
    ) }}
