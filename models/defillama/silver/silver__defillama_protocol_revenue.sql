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
                'https://pro-api.llama.fi/{api_key}/api/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyRevenue',{},{},'Vault/prod/defillama'
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
        k.value AS rev_object,
        v.value :: INTEGER AS daily_rev,
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
    daily_rev,
    rev_object,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['chain', 'protocol', 'timestamp']
    ) }} AS id
FROM
    chain_breakdown
