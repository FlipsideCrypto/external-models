{{ config(
    materialized = 'incremental',
    unique_key = ['protocol_id', 'chain', 'date'],
    cluster_by = ['chain'],
    tags = ['defillama']
) }}

WITH lat_flat AS (

    SELECT
        protocol_id,
        category,
        r.key AS chain,
        r.value AS VALUE,
        _inserted_timestamp
    FROM
        {{ ref('bronze__defillama_protocol_tvl_history') }},
        LATERAL FLATTEN (response) AS r
{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            max(_inserted_timestamp)
        FROM
            {{ this }}
        )
{% endif %}
),
lat_flat2 AS (
    SELECT
        protocol_id,
        category,
        chain,
        r.value :date AS unix_timestamp,
        DATE_TRUNC(
            'HOUR',
            TIMEADD(
                SECOND,
                r.value :date :: INT,
                '1970-01-01' :: timestamp_ntz
            )
        ) AS converted_date,
        r.value :totalLiquidityUSD AS total_liquidity_usd,
        _inserted_timestamp
    FROM
        lat_flat,
        LATERAL FLATTEN (
            VALUE :tvl
        ) AS r
)
SELECT
    converted_date AS DATE,
    protocol_id,
    category,
    chain,
    total_liquidity_usd :: INT AS total_liquidity_usd,
    {{ dbt_utils.generate_surrogate_key(
        ['protocol_id','chain','date']
    ) }} AS defillama_protocol_tvl_history_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    lat_flat2
qualify(ROW_NUMBER() over (PARTITION BY protocol_id, chain, date
ORDER BY
    _inserted_timestamp DESC
)) = 1