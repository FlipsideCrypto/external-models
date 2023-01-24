{{ config(
    materialized = 'view',
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
        'calls'
    ) }}
