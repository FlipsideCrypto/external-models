{{ config(
    materialized = 'incremental',
    full_refresh = false,
    unique_key = ['protocol_id','chain','timestamp'],
    cluster_by = ['chain'],
    tags = ['defillama']
) }}

WITH api_pull AS (

    SELECT
        PARSE_JSON(
            live.udf_api(
                'GET',
                'https://api.llama.fi/lite/protocols2?b=2',{},{}
            )
        ) :data :protocols AS response,
        SYSDATE() AS _inserted_timestamp
),
lat_flat AS (
    SELECT
        r.value AS VALUE,
        _inserted_timestamp
    FROM
        api_pull,
        LATERAL FLATTEN (response) AS r
),
protocol_expand AS (
    SELECT
        SYSDATE() :: DATE AS TIMESTAMP,
        VALUE :defillamaId :: STRING AS protocol_id,
        VALUE :category :: STRING AS category,
        VALUE :name :: STRING AS NAME,
        VALUE :mcap :: FLOAT AS market_cap,
        VALUE :symbol :: STRING AS symbol,
        VALUE :chains AS chains,
        VALUE :tvl :: FLOAT AS tvl,
        VALUE :tvlPrevDay :: FLOAT AS tvl_prev_day,
        VALUE :tvlPrevWeek :: FLOAT AS tvl_prev_week,
        VALUE :tvlPrevMonth :: FLOAT AS tvl_prev_month,
        VALUE :chainTvls AS chain_tvls,
        _inserted_timestamp
    FROM
        lat_flat

{% if is_incremental() %}
WHERE
    _inserted_timestamp :: DATE > (
        SELECT
            MAX(_inserted_timestamp) :: DATE
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    timestamp,
    protocol_id,
    category,
    NAME,
    market_cap,
    symbol,
    tvl,
    tvl_prev_day,
    tvl_prev_week,
    tvl_prev_month,
    r.key AS chain,
    r.value :tvl :: INT AS chain_tvl,
    r.value: tvlPrevDay :: INT AS chain_tvl_prev_day,
    r.value: tvlPrevWeek :: INT AS chain_tvl_prev_week,
    r.value: tvlPrevMonth :: INT AS chain_tvl_prev_month,
    _inserted_timestamp
FROM
    protocol_expand,
    LATERAL FLATTEN (chain_tvls) AS r
