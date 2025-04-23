{{ config(
  materialized = 'incremental',
  unique_key = ['date', 'protocol_id', 'chain'],
  cluster_by = ['date', 'protocol_id'],
  tags = ['defillama']
) }}

WITH daily_close AS (

  SELECT
    DATE_TRUNC(
      'day',
      DATE
    ) AS DATE,
    protocol_id,
    chain,
    LAST_VALUE(total_liquidity_usd) over (PARTITION BY DATE_TRUNC('day', DATE), protocol_id, chain
  ORDER BY
    DATE rows BETWEEN unbounded preceding
    AND unbounded following) AS chain_tvl,
    _inserted_timestamp
  FROM
    {{ ref('silver__defillama_protocol_tvl_history') }}

{% if is_incremental() %}
WHERE
  DATE >= (
    SELECT
      DATE_TRUNC('day', MAX(DATE))
    FROM
      {{ this }})
    {% endif %}

    qualify ROW_NUMBER() over (PARTITION BY DATE_TRUNC('day', DATE), protocol_id, chain
    ORDER BY
      DATE DESC) = 1
  ),
  daily_tvl AS (
    SELECT
      DATE,
      protocol_id,
      chain,
      chain_tvl,
      LAG(
        chain_tvl,
        1
      ) over (
        PARTITION BY protocol_id,
        chain
        ORDER BY
          DATE
      ) AS chain_tvl_prev_day,
      LAG(
        chain_tvl,
        7
      ) over (
        PARTITION BY protocol_id,
        chain
        ORDER BY
          DATE
      ) AS chain_tvl_prev_week,
      LAG(
        chain_tvl,
        30
      ) over (
        PARTITION BY protocol_id,
        chain
        ORDER BY
          DATE
      ) AS chain_tvl_prev_month,
      _inserted_timestamp
    FROM
      daily_close
  )
SELECT
  d.date,
  d.chain,
  d.protocol_id,
  p.category,
  p.protocol,
  p2.market_cap,
  p.symbol,
  d.chain_tvl,
  d.chain_tvl_prev_day,
  d.chain_tvl_prev_week,
  d.chain_tvl_prev_month,
  d._inserted_timestamp
FROM
  daily_tvl d
  LEFT JOIN {{ ref('bronze__defillama_protocols') }}
  p
  ON p.protocol_id = d.protocol_id
  LEFT JOIN {{ ref('silver__defillama_protocol_tvl') }}
  p2
  ON p2.protocol_id = d.protocol_id
  AND p2.chain = d.chain
  AND p2.timestamp = d.date

{% if is_incremental() %}
WHERE
  DATE > (
    SELECT
      MAX(DATE)
    FROM
      {{ this }}
  )
{% endif %}
