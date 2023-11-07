{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    full_refresh = false,
    tags = ['defillama']
) }}

WITH all_chains_dex_base AS (

SELECT
    LOWER(VALUE::STRING) AS chain,
    ROW_NUMBER() OVER (ORDER BY chain) AS row_num, 
    _inserted_timestamp
FROM (
    SELECT
        ethereum.streamline.udf_api(
            'GET','https://api.llama.fi/overview/dexs?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume',{},{}
            ) AS read,
        SYSDATE() AS _inserted_timestamp
    ),
LATERAL FLATTEN (input=> read:data:allChains) 
),

dex_base AS (

{% for item in range(10) %}
(
SELECT
    chain,
    ethereum.streamline.udf_api(
        'GET',CONCAT('https://api.llama.fi/overview/dexs/',chain,'?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=false&dataType=dailyVolume'),{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp
FROM (
    SELECT 
        DISTINCT chain, 
        row_num
    FROM all_chains_dex_base
    WHERE row_num BETWEEN {{ item * 20 + 1 }} AND {{ (item + 1) * 20 }}
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
),

reads_output AS (

SELECT
    chain,
    TO_TIMESTAMP(VALUE[0]::INTEGER) AS timestamp,
    VALUE[1] AS dex_object,
    _inserted_timestamp
FROM dex_base,
    LATERAL FLATTEN (input=> read:data:totalDataChartBreakdown)
)

SELECT
    chain,
    timestamp,
    LOWER(key::STRING) AS protocol, 
    value::INTEGER AS daily_volume,
    dex_object,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['chain', 'protocol', 'timestamp']
    ) }} AS id
FROM reads_output,
  LATERAL FLATTEN(input => PARSE_JSON(reads_output.dex_object))
