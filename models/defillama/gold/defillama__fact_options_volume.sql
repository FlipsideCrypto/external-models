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
    p.TIMESTAMP :: DATE AS DATE,
    p.chain,
    p.protocol,
    COALESCE(daily_volume_premium,0) AS volume_premium,
    COALESCE(daily_volume_notional,0) AS volume_notional
FROM
    {{ ref('silver__defillama_options_premium') }} p
LEFT JOIN
    {{ ref('silver__defillama_options_notional') }} n
    ON p.TIMESTAMP = n.TIMESTAMP AND p.chain = n.chain AND p.protocol = n.protocol
