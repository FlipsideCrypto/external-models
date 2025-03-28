{{ config(
    materialized = 'incremental',
    full_refresh = true,
    unique_key = ['protocol_id', 'chain', 'unix_timestamp'],
    cluster_by = ['chain'],
    tags = ['defillama']
) }}

with api_pull AS (
    SELECT
        'aave' as protocol_id,
        live.udf_api(
            'GET','https://pro-api.llama.fi/{api_key}/api/protocol/'||protocol_id
            ,{},{},
            'Vault/prod/external/defillama'
        ):data:chainTvls AS response,
        SYSDATE() AS _inserted_timestamp
),
lat_flat AS (
    SELECT
        protocol_id,
        r.key AS chain,
        r.value AS VALUE,
        _inserted_timestamp
    FROM
        api_pull,
        LATERAL FLATTEN (response) AS r
),
lat_flat2 AS (
    SELECT
        protocol_id,
        chain,
        r.value:date AS unix_timestamp,
        DATE_TRUNC('HOUR', TIMEADD(second, r.value:date::int, '1970-01-01'::timestamp_ntz)) AS CONVERTED_DATE,
        r.value:totalLiquidityUSD AS total_liquidity_usd,
        _inserted_timestamp
    FROM
        lat_flat,
        LATERAL FLATTEN (value:tvl) AS r
)
SELECT 
    CONVERTED_DATE as date,
    protocol_id,
    chain,
    unix_timestamp,
    total_liquidity_usd::int as total_liquidity_usd,
    _inserted_timestamp
FROM lat_flat2


