{{ config(
    materialized = 'incremental',
    unique_key = ['stablecoin_id','timestamp'],
    full_refresh = false,
    tags = ['defillama']
) }}
WITH stablecoin_base AS (

{% for item in range(50) %}
(
SELECT
    stablecoin_id,
    stablecoin,
    symbol,
    live.udf_api(
        'GET',concat('https://stablecoins.llama.fi/stablecoin/',stablecoin_id),{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp,
FROM (
    SELECT 
        stablecoin_id,
        stablecoin,
        symbol,
        row_num
    FROM {{ ref('bronze__defillama_stablecoins') }}
    WHERE row_num BETWEEN {{ item * 5 + 1 }} AND {{ (item + 1) * 5 }}
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
    ))
{% endif %}
){% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
)
SELECT
    read:data:address::string as address,
    read:data:symbol::string as symbol,
    read:data:name::string as name,
    read:data:id::string as stablecoin_id,
    value AS value,
    TO_TIMESTAMP(VALUE:date::INTEGER) AS timestamp,
    _inserted_timestamp
FROM stablecoin_base,
    LATERAL FLATTEN (input=> read:data:tokens) f
