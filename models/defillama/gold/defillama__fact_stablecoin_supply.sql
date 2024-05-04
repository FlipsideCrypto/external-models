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
    stablecoin_id,
    name AS stablecoin,
    chains,
    symbol,
    circulating,
    minted,
    unreleased
FROM
    {{ ref('silver__defillama_stablecoin_supply') }} f
