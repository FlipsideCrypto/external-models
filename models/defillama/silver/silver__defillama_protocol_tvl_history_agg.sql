{{ config(
  materialized = 'incremental',
  unique_key = ['protocol_tvl_history_agg_id'],
  cluster_by = ['timestamp', 'protocol_id'],
  tags = ['defillama']
) }}

WITH daily_tvl AS (
  SELECT
    date,
    protocol_id,
    chain,
    total_liquidity_usd AS chain_tvl,
    _inserted_timestamp
  FROM
    {{ ref('silver__defillama_protocol_tvl_history') }}

  {% if is_incremental() %}
  WHERE
    _inserted_timestamp >= (
      SELECT
        MAX(_inserted_timestamp)
      FROM
        {{ this }})
  {% endif %}

  -- Since data is already at date level, we just need to take the latest record per date/protocol/chain
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY date, protocol_id, chain
    ORDER BY _inserted_timestamp DESC
  ) = 1
),

daily_tvl_with_lags AS (
  SELECT
    date::DATE as timestamp,
    protocol_id,
    chain,
    chain_tvl,
    LAG(chain_tvl, 1) OVER (
      PARTITION BY protocol_id, chain
      ORDER BY date
    ) AS chain_tvl_prev_day,
    LAG(chain_tvl, 7) OVER (
      PARTITION BY protocol_id, chain
      ORDER BY date
    ) AS chain_tvl_prev_week,
    LAG(chain_tvl, 30) OVER (
      PARTITION BY protocol_id, chain
      ORDER BY date
    ) AS chain_tvl_prev_month,
    _inserted_timestamp
  FROM
    daily_tvl
),

protocol_total_tvl AS (
  SELECT
    timestamp,
    protocol_id,
    SUM(chain_tvl) AS tvl,
    SUM(chain_tvl_prev_day) AS tvl_prev_day,
    SUM(chain_tvl_prev_week) AS tvl_prev_week,
    SUM(chain_tvl_prev_month) AS tvl_prev_month
  FROM
    daily_tvl_with_lags
  GROUP BY
    timestamp, protocol_id
)

SELECT
  d.timestamp,
  p.protocol_id,
  p.category,
  p.protocol,
  NULL AS market_cap,
  p.symbol,
  pt.tvl,
  pt.tvl_prev_day,
  pt.tvl_prev_week,
  pt.tvl_prev_month,
  d.chain,
  d.chain_tvl,
  d.chain_tvl_prev_day,
  d.chain_tvl_prev_week,
  d.chain_tvl_prev_month,
  d._inserted_timestamp,
  {{ dbt_utils.generate_surrogate_key(
      ['d.protocol_id','d.chain','d.timestamp']
  ) }} AS protocol_tvl_history_agg_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  daily_tvl_with_lags d
  LEFT JOIN {{ ref('bronze__defillama_protocols') }} p
    ON p.protocol_id = d.protocol_id
  LEFT JOIN protocol_total_tvl pt
    ON pt.protocol_id = d.protocol_id
    AND pt.timestamp = d.timestamp
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY d.timestamp, d.protocol_id, d.chain
  ORDER BY d._inserted_timestamp DESC
) = 1
