{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    full_refresh = false,
    tags = ['defillama']
) }}

WITH tvl_base AS (

{% for item in range(5) %}
(
SELECT
    chain,
    ethereum.streamline.udf_api(
        'GET',CONCAT('https://api.llama.fi/charts/',chain),{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp
FROM (
    SELECT 
        DISTINCT chain, 
        row_num
    FROM {{ ref('bronze__defillama_chains') }}
    WHERE row_num BETWEEN {{ item * 60 + 1 }} AND {{ (item + 1) * 60 }}
    )
{% if is_incremental() %}
WHERE chain NOT IN (
    SELECT
        chain
    FROM (
        SELECT 
            DISTINCT chain,
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
    chain,
    TO_TIMESTAMP(VALUE:date::INTEGER) AS timestamp,
    VALUE:totalLiquidityUSD::INTEGER AS tvl_usd,
    _inserted_timestamp,
     {{ dbt_utils.surrogate_key(
        ['chain', 'timestamp']
    ) }} AS id
FROM tvl_base,
    LATERAL FLATTEN (input=> read:data)