{{ config(
    materialized = 'incremental',
    unique_key = ['date', 'protocol_id', 'chain'],
    cluster_by = ['date', 'protocol_id'],
    tags = ['defillama']
) }}

WITH daily_close AS (
  SELECT 
    DATE_TRUNC('day', date) as date,
    protocol_id,
    chain,
    LAST_VALUE(total_liquidity_usd) OVER (
      PARTITION BY DATE_TRUNC('day', date), protocol_id, chain 
      ORDER BY date
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as chain_tvl,
    _inserted_timestamp
  FROM {{ ref('bronze__defillama_protocol_tvl_history') }}
  {% if is_incremental() %}
  WHERE date >= (SELECT DATE_TRUNC('day', MAX(date)) FROM {{ this }})
  {% endif %}
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY DATE_TRUNC('day', date), protocol_id, chain 
    ORDER BY date DESC
  ) = 1
),

daily_tvl AS (
  SELECT 
    date,
    protocol_id,
    chain,
    chain_tvl,
    LAG(chain_tvl, 1) OVER (PARTITION BY protocol_id, chain ORDER BY date) as chain_tvl_prev_day,
    LAG(chain_tvl, 7) OVER (PARTITION BY protocol_id, chain ORDER BY date) as chain_tvl_prev_week,
    LAG(chain_tvl, 30) OVER (PARTITION BY protocol_id, chain ORDER BY date) as chain_tvl_prev_month,
    _inserted_timestamp
  FROM daily_close
)

SELECT 
  d.date,
  d.chain,
  d.protocol_id,
  p.category,
  p.protocol,
  NULL AS market_cap,
  p.symbol,
  d.chain_tvl,
  d.chain_tvl_prev_day,
  d.chain_tvl_prev_week,
  d.chain_tvl_prev_month,
  d._inserted_timestamp
FROM daily_tvl d
LEFT JOIN {{ ref('bronze__defillama_protocols') }} p
  ON p.protocol_id = d.protocol_id
{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}