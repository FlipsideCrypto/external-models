{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['defillama']
) }}

SELECT
    TIMESTAMP :: DATE AS DATE,
    stablecoin_id,
    stablecoin,
    symbol,
    circulating_usd,
    minted,
    unreleased_usd,
    bridged_to
FROM
    {{ ref('silver__defillama_stablecoin_supply') }}
