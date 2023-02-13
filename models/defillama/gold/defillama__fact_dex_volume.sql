{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['defillama']
) }}

SELECT
    TIMESTAMP :: DATE AS DATE,
    chain,
    protocol,
    daily_volume AS volume
FROM
    {{ ref('silver__defillama_dex_volume') }} f
