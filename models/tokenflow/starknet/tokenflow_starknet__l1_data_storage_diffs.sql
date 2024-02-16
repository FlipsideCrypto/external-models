{{ config(
    materialized = 'view',
    tags = ['tokenflow'],
    persist_docs ={ "relation": true,
    "columns": true },
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
        'l1_storage_diffs'
    ) }}
