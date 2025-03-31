{{ config(
    materialized = 'incremental',
    unique_key = ['protocol_id', 'chain', 'unix_timestamp'],
    cluster_by = ['chain'],
    tags = ['defillama']
) }}

WITH protocol_base AS (

    SELECT
        protocol_slug, 
        protocol_id,
        row_num,
        live.udf_api(
            'GET','https://pro-api.llama.fi/{api_key}/api/hourly/'||protocol_slug
            ,{},{},
            'Vault/prod/external/defillama'
        ):data:chainTvls AS response,
        SYSDATE() AS _inserted_timestamp
    FROM
        (
    SELECT
        protocol_slug,
        protocol_id,
        row_num
    FROM
        {{ ref('bronze__defillama_protocols') }}
    {% if is_incremental() %}
    WHERE
        row_num >= (select max(row_num) from {{ this }})+1
    ORDER BY 
        row_num ASC
    limit 5
    {% else %}
    WHERE
        row_num = 1
    {% endif %}
    )
), 

lat_flat AS (
    SELECT
        protocol_slug,
        protocol_id,
        row_num,
        r.key AS chain,
        r.value AS VALUE,
        _inserted_timestamp
    FROM
        protocol_base,
        LATERAL FLATTEN (response) AS r
),
lat_flat2 AS (
    SELECT
        protocol_slug,
        protocol_id,
        row_num,
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
    protocol_slug,
    protocol_id,
    row_num,
    chain,
    unix_timestamp,
    total_liquidity_usd::int as total_liquidity_usd,
    _inserted_timestamp
FROM lat_flat2


