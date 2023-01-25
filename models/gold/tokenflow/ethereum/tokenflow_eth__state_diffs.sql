{{ config(
    materialized = 'view',
    tags = ['tokenflow'],
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'TOKENFLOW, ETHEREUM'
            }
        }
    }
) }}

SELECT
    *
FROM
    {{ source(
        'tokenflow_eth',
        'state_diffs'
    ) }}
