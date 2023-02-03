{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    full_refresh = false,
    tags = ['defillama']
) }}

WITH chain_base AS (

SELECT DISTINCT chain
FROM {{ ref('bronze__defillama_chains') }}
),

tvl_base_1 AS (

SELECT
    chain,
    ethereum.streamline.udf_api(
        'GET',CONCAT('https://api.llama.fi/charts/',chain),{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp
FROM chain_base
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
    ) c
)
{% endif %}
LIMIT 60
),

tvl_base_2 AS (

SELECT
    chain,
    ethereum.streamline.udf_api(
        'GET',CONCAT('https://api.llama.fi/charts/',chain),{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp
FROM chain_base
WHERE chain NOT IN (
    SELECT
        DISTINCT chain
    FROM tvl_base_1
    ) 
{% if is_incremental() %}
AND chain NOT IN (
    SELECT
        chain
    FROM (
        SELECT 
            DISTINCT chain,
            MAX(timestamp::DATE) AS max_timestamp
        FROM {{ this }}
        GROUP BY 1
        HAVING CURRENT_DATE = max_timestamp
    ) c
)
{% endif %}
LIMIT 60
),

tvl_base_3 AS (

SELECT
    chain,
    ethereum.streamline.udf_api(
        'GET',CONCAT('https://api.llama.fi/charts/',chain),{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp
FROM chain_base
WHERE chain NOT IN (
    SELECT
        DISTINCT chain
    FROM tvl_base_1
    UNION ALL
    SELECT
        DISTINCT chain
    FROM tvl_base_2
    ) 
{% if is_incremental() %}
AND chain NOT IN (
    SELECT
        chain
    FROM (
        SELECT 
            DISTINCT chain,
            MAX(timestamp::DATE) AS max_timestamp
        FROM {{ this }}
        GROUP BY 1
        HAVING CURRENT_DATE = max_timestamp
    ) c
)
{% endif %}
LIMIT 60
),

tvl_base_4 AS (

SELECT
    chain,
    ethereum.streamline.udf_api(
        'GET',CONCAT('https://api.llama.fi/charts/',chain),{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp
FROM chain_base
WHERE chain NOT IN (
    SELECT
        DISTINCT chain
    FROM tvl_base_1
    UNION ALL
    SELECT
        DISTINCT chain
    FROM tvl_base_2
    UNION ALL
    SELECT
        DISTINCT chain
    FROM tvl_base_3
    ) 
{% if is_incremental() %}
AND chain NOT IN (
    SELECT
        chain
    FROM (
        SELECT 
            DISTINCT chain,
            MAX(timestamp::DATE) AS max_timestamp
        FROM {{ this }}
        GROUP BY 1
        HAVING CURRENT_DATE = max_timestamp
    ) c
)
{% endif %}
LIMIT 60
),

chain_tvl_all AS (
SELECT *
FROM tvl_base_1
UNION ALL
SELECT *
FROM tvl_base_2
UNION ALL
SELECT *
FROM tvl_base_3
UNION ALL
SELECT *
FROM tvl_base_4
)

SELECT
    chain,
    TO_TIMESTAMP(VALUE:date::INTEGER) AS timestamp,
    VALUE:totalLiquidityUSD::INTEGER AS tvl_usd,
    _inserted_timestamp,
     {{ dbt_utils.surrogate_key(
        ['chain', 'timestamp']
    ) }} AS id
FROM chain_tvl_all,
    LATERAL FLATTEN (input=> read:data)