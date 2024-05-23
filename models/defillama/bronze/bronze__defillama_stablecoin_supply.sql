{{ config(
    materialized = 'incremental',
    unique_key = ['stablecoin_id','timestamp'],
    full_refresh = false,
    tags = ['defillama']
) }}

WITH stablecoin_base AS ({% for item in range(50) %}
    (

    SELECT
        stablecoin_id, 
        stablecoin, 
        symbol, 
        live.udf_api('GET',
            CONCAT('https://stablecoins.llama.fi/stablecoin/', stablecoin_id),{},{}) AS READ, 
        SYSDATE() AS _inserted_timestamp,
    FROM
        (
    SELECT
        stablecoin_id, 
        stablecoin, 
        symbol, 
        row_num
    FROM
        {{ ref('bronze__defillama_stablecoins') }}
    WHERE
        row_num BETWEEN {{ item * 5 + 1 }}
        AND {{(item + 1) * 5 }}
        )

{% if is_incremental() %}
WHERE
    stablecoin_id NOT IN (
SELECT
    stablecoin_id
FROM
    (
SELECT
    DISTINCT stablecoin_id, 
    MAX(TIMESTAMP :: DATE) AS max_timestamp
FROM
    {{ this }}
GROUP BY
    1
HAVING
    CURRENT_DATE = max_timestamp))
{% endif %}) {% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %})
SELECT
    READ :data :address :: STRING AS address,
    READ :data :symbol :: STRING AS symbol,
    READ :data :name :: STRING AS NAME,
    READ :data :id :: STRING AS stablecoin_id,
    VALUE AS VALUE,
    TO_TIMESTAMP(
        VALUE :date :: INTEGER
    ) AS TIMESTAMP,
    _inserted_timestamp
FROM
    stablecoin_base,
    LATERAL FLATTEN (
        input => READ :data :tokens
    ) f
