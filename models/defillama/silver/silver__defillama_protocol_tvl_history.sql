{{ config(
    materialized = 'incremental',
    unique_key = ['protocol_id', 'chain', 'timestamp'],
    cluster_by = ['timestamp', 'protocol_id', 'chain'],
    tags = ['defillama_history']
) }}

WITH tvl_history AS (
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

daily_tvl_data AS (
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
        )::DATE AS timestamp,
        r.value :totalLiquidityUSD::INT AS chain_tvl,
        _inserted_timestamp
    FROM
        tvl_history,
        LATERAL FLATTEN (
            VALUE :tvl
        ) AS r
),

daily_tvl_with_lags AS (
    SELECT
        timestamp,
        protocol_id,
        category,
        chain,
        chain_tvl,
        LAG(chain_tvl, 1) OVER (
          PARTITION BY protocol_id, chain
          ORDER BY timestamp
        ) AS chain_tvl_prev_day,
        LAG(chain_tvl, 7) OVER (
          PARTITION BY protocol_id, chain
          ORDER BY timestamp
        ) AS chain_tvl_prev_week,
        LAG(chain_tvl, 30) OVER (
          PARTITION BY protocol_id, chain
          ORDER BY timestamp
        ) AS chain_tvl_prev_month,
        _inserted_timestamp
    FROM
        daily_tvl_data
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY timestamp, protocol_id, chain
      ORDER BY _inserted_timestamp DESC
    ) = 1
)

SELECT
    d.timestamp,
    d.protocol_id,
    d.category,
    p.protocol,
    p.symbol,
    d.chain,
    d.chain_tvl,
    d.chain_tvl_prev_day,
    d.chain_tvl_prev_week,
    d.chain_tvl_prev_month,
    d._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['d.protocol_id','d.chain','d.timestamp']
    ) }} AS defillama_protocol_tvl_history_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    daily_tvl_with_lags d
    LEFT JOIN {{ ref('defillama__dim_protocols') }} p
      ON p.protocol_id = d.protocol_id
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY d.timestamp, d.protocol_id, d.chain
    ORDER BY d._inserted_timestamp DESC
) = 1