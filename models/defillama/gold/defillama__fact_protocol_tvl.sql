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
    TIMESTAMP :: DATE AS DATE,
    chain,
    protocol_id,
    category,
    protocol,
    market_cap,
    symbol,
    chain_tvl,
    chain_tvl_prev_day,
    chain_tvl_prev_week,
    chain_tvl_prev_month
FROM
    {{ ref('silver__defillama_protocol_tvl') }} f
