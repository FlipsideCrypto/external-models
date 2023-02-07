{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    full_refresh = false,
    tags = ['defillama']
) }}

WITH stablecoin_base AS (

{% for item in range(15) %}
(
SELECT
    stablecoin_id,
    stablecoin,
    symbol,
    ethereum.streamline.udf_api(
        'GET',CONCAT('https://stablecoins.llama.fi/stablecoin/',stablecoin_id),{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp
FROM (
    SELECT 
        stablecoin_id,
        stablecoin,
        symbol,
        row_num
    FROM {{ ref('bronze__defillama_stablecoins') }}
    WHERE row_num BETWEEN {{ item * 10 + 1 }} AND {{ (item + 1) * 10}}
        AND symbol NOT IN ('USDT','USDC')
    )
{% if is_incremental() %}
WHERE stablecoin_id NOT IN (
    SELECT
        stablecoin_id
    FROM (
        SELECT 
            DISTINCT stablecoin_id,
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
    stablecoin_id,
    stablecoin,
    symbol,
    TO_TIMESTAMP(VALUE:date::INTEGER) AS timestamp,
    VALUE:circulating:peggedUSD::INTEGER AS circulating_usd,
    VALUE:minted::INTEGER AS minted,
    VALUE:unreleased:peggedUSD::INTEGER AS unreleased_usd,
    VALUE:bridgedTo::INTEGER AS bridged_to,
    _inserted_timestamp,
    CONCAT(stablecoin_id,'-',symbol,'-',timestamp) AS id
FROM stablecoin_base,
    LATERAL FLATTEN (input=> read:data:tokens) 