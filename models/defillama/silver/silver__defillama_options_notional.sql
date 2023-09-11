{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    full_refresh = false,
    tags = ['defillama']
) }}

WITH all_chains_options_base AS (

SELECT
    LOWER(VALUE::STRING) AS chain,
    ROW_NUMBER() OVER (ORDER BY chain) AS row_num, 
    _inserted_timestamp
FROM (
    SELECT
        ethereum.streamline.udf_api(
            'GET','https://api.llama.fi/overview/options?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyNotionalVolume',{},{}
            ) AS read,
        SYSDATE() AS _inserted_timestamp
    ),
LATERAL FLATTEN (input=> read:data:allChains) 
),

options_base AS (

{% for item in range(5) %}
(
SELECT
    chain,
    ethereum.streamline.udf_api(
        'GET',CONCAT('https://api.llama.fi/overview/options/',chain,'?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=false&dataType=dailyNotionalVolume'),{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp
FROM (
    SELECT 
        DISTINCT chain, 
        row_num
    FROM all_chains_options_base
    WHERE row_num BETWEEN {{ item * 5 + 1 }} AND {{ (item + 1) * 5 }}
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
    VALUE[1] AS options_object,
    _inserted_timestamp
FROM options_base,
    LATERAL FLATTEN (input=> read:data:totalDataChartBreakdown)
)

SELECT
    chain,
    timestamp,
    key::STRING AS protocol, 
    value::INTEGER AS daily_volume_notional,
    options_object,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['chain', 'protocol', 'timestamp']
    ) }} AS id
FROM reads_output,
  LATERAL FLATTEN(input => PARSE_JSON(reads_output.options_object))