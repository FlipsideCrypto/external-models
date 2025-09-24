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
    blockchain as chain,
    protocol,
    protocol_slug,
    protocol_id,
    volume
FROM
    {{ ref('silver__defillama_perp_daily_volume') }} f
