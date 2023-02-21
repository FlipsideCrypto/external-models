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
    f.TIMESTAMP :: DATE AS DATE,
    f.chain,
    f.protocol,
    COALESCE(daily_fees,0) AS fees,
    COALESCE(daily_rev,0) AS revenue
FROM
    {{ ref('silver__defillama_protocol_fees') }} f
LEFT JOIN
    {{ ref('silver__defillama_protocol_revenue') }} r
    ON f.TIMESTAMP = r.TIMESTAMP AND f.chain = r.chain AND f.protocol = r.protocol
