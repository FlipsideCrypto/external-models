{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    tags = ['defillama']
) }}

WITH stablecoin_base AS (

{% for item in range(4) %}
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
    FROM external_dev.bronze.defillama_stablecoins
    WHERE row_num BETWEEN {{ item * 20 + 1 }} AND {{ (item + 1) * 20 }} and stablecoin_id not in (1,2))
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
UNION ALL
(
SELECT
    stablecoin_id,
    stablecoin,
    symbol,
    live.udf_api(
        'GET','https://stablecoins.llama.fi/stablecoin/1',{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp,
FROM (
    SELECT 
        stablecoin_id,
        stablecoin,
        symbol,
        row_num
    FROM external_dev.bronze.defillama_stablecoins
    WHERE stablecoin_id =1)
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
)
union all
(
SELECT
    stablecoin_id,
    stablecoin,
    symbol,
    live.udf_api(
        'GET','https://stablecoins.llama.fi/stablecoin/2',{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp,
FROM (
    SELECT 
        stablecoin_id,
        stablecoin,
        symbol,
        row_num
    FROM external_dev.bronze.defillama_stablecoins
    WHERE stablecoin_id =2)
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
)
)

SELECT
    read:data:address::string as address,
    read:data:symbol::string as symbol,
    read:data:name::string as name,
    read:data:id::string as stablecoin_id,
    value AS chain,
    TO_TIMESTAMP(VALUE:date::INTEGER) AS timestamp,
    value:circulating:peggedUSD::INTEGER AS circulating,
    value:minted:peggedUSD::INTEGER AS minted,
    value:unreleased:peggedUSD::INTEGER AS unrelease,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['stablecoin_id', 'chain', 'timestamp']
    ) }} AS id
FROM stablecoin_base,
    LATERAL FLATTEN (input=> read:data:tokens)