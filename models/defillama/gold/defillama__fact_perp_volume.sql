{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['defillama'],
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'DEFILLAMA'
            }
        }
    }
) }}

SELECT
    DATE,
    chain,
    protocol_id,
    protocol_slug,
    protocol,
    volume
FROM
    {{ ref('silver__defillama_perp_daily_volume') }} f
