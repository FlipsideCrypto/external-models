{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    full_refresh = false,
    tags = ['defillama']
) }}

WITH bridge_base AS (

{% for item in range(5) %}
(
SELECT
    bridge_id,
    bridge,
    bridge_name,
    ethereum.streamline.udf_api(
        'GET',CONCAT('https://bridges.llama.fi/bridgevolume/all?id=',bridge_id),{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp
FROM (
    SELECT 
        bridge_id,
        bridge,
        bridge_name,
        row_num
    FROM {{ ref('bronze__defillama_bridges') }}
    WHERE row_num BETWEEN {{ item * 10 + 1 }} AND {{ (item + 1) * 10}}
    )
{% if is_incremental() %}
WHERE bridge_id NOT IN (
    SELECT
        bridge_id
    FROM (
        SELECT 
            DISTINCT bridge_id,
            MAX(timestamp::DATE) AS max_timestamp
        FROM {{ this }}
        GROUP BY 1
        HAVING CURRENT_DATE = max_timestamp
    )
)
{% endif %}
) {% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
)

SELECT
    bridge_id,
    bridge,
    bridge_name,
    TO_TIMESTAMP(VALUE:date::INTEGER) AS timestamp,
    VALUE:depositTxs::INTEGER AS deposit_txs,
    VALUE:depositUSD::INTEGER AS deposit_usd,
    VALUE:withdrawTxs::INTEGER AS withdraw_txs,
    VALUE:withdrawUSD::INTEGER AS withdraw_usd,
    _inserted_timestamp,
    CONCAT(bridge_id,'-',bridge,'-',timestamp) AS id
FROM bridge_base,
    LATERAL FLATTEN (input=> read:data)
WHERE id IS NOT NULL