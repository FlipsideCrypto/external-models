{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    full_refresh = false,
    tags = ['defillama']
) }}

WITH api_pull AS (

    SELECT
        PARSE_JSON(
            live.udf_api(
                'GET',
                'https://api.llama.fi/overview/dexs?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume',{},{}
            )
        ) :data :protocols AS response,
        SYSDATE() AS _inserted_timestamp
),
lat_flat AS (
    SELECT
        r.value AS VALUE,
        r.VALUE:displayName::STRING AS protocol,
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
        k.value AS dex_object,
        v.value :: INTEGER AS daily_volume,
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
    LOWER(protocol) AS protocol,
    daily_volume,
    dex_object,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['chain', 'protocol', 'timestamp']
    ) }} AS id
FROM
    chain_breakdown
