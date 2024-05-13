{{ config(
    materialized = 'incremental',
    unique_key = 'defillama_historical_yields_id',
    tags = ['defillama']
) }}

WITH yield_union AS(

    SELECT
        *
    FROM
        {{ ref('bronze__defillama_historical_yields_100') }}
{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
    UNION ALL
    SELECT
        *
    FROM
        {{ ref('bronze__defillama_historical_yields_101_200') }}
{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
    UNION ALL
    SELECT
        *
    FROM
        {{ ref('bronze__defillama_historical_yields_201_300') }}
{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
    UNION ALL
    SELECT
        *
    FROM
        {{ ref('bronze__defillama_historical_yields_301_400') }}
{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
    UNION ALL
    SELECT
        *
    FROM
        {{ ref('bronze__defillama_historical_yields_401_500') }}
)
SELECT
    *
FROM
    yield_union qualify(ROW_NUMBER() over(PARTITION BY defillama_historical_yields_id
ORDER BY
    _inserted_timestamp DESC)) = 1

