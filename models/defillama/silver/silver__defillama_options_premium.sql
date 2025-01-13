{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    enabled = false,
    full_refresh = false,
    tags = ['defillama']
) }}

WITH api_pull AS (

    SELECT
        PARSE_JSON(
            live.udf_api(
                'GET',
                'https://api.llama.fi/overview/options?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyPremiumVolume',{},{}
            )
        ) :data :protocols AS response,
        SYSDATE() AS _inserted_timestamp
),
lat_flat AS (
    SELECT
        r.value AS VALUE,
        r.value :displayName :: STRING AS protocol,
        _inserted_timestamp
    FROM
        api_pull,
        LATERAL FLATTEN (
            input => response
        ) AS r
),
chain_breakdown AS (
    SELECT
        k.key AS chain,
        SYSDATE() :: DATE AS TIMESTAMP,
        protocol,
        k.value AS options_object,
        v.value :: INTEGER AS daily_volume_premium,
        _inserted_timestamp
    FROM
        lat_flat,
        LATERAL FLATTEN(
            input => VALUE :breakdown24h
        ) k,
        LATERAL FLATTEN(
            input => k.value
        ) v
)
SELECT
    chain,
    TIMESTAMP,
    protocol,
    daily_volume_premium,
    options_object,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['chain', 'protocol', 'timestamp']
    ) }} AS id
FROM
    chain_breakdown
