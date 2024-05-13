{{ config(
    materialized = 'incremental',
    unique_key = 'defillama_historical_yield_id',
    tags = ['defillama']
) }}

WITH yield_union AS(

    SELECT
        *
    FROM
        {{ ref('bronze__defillama_historical_yields_100') }}
    UNION ALL
    SELECT
        *
    FROM
        {{ ref('bronze__defillama_historical_yields_101_200') }}
    UNION ALL
    SELECT
        *
    FROM
        {{ ref('bronze__defillama_historical_yields_201_300') }}
    UNION ALL
    SELECT
        *
    FROM
        {{ ref('bronze__defillama_historical_yields_301_400') }}
    UNION ALL
    SELECT
        *
    FROM
        {{ ref('bronze__defillama_historical_yields_401_500') }}
)
SELECT
    *
FROM
    yield_union
