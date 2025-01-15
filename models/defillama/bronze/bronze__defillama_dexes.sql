{{ config(
    materialized = 'incremental',
    unique_key = 'dex_id',
    tags = ['defillama']
) }}

WITH base AS (

SELECT
    live.udf_api(
        'GET','https://pro-api.llama.fi/{api_key}/api/overview/dexs?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume',{},{},'Vault/prod/defillama'
    ) AS dex_read,
    live.udf_api(
        'GET','https://pro-api.llama.fi/{api_key}/api/overview/options?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyPremiumVolume',{},{},'Vault/prod/defillama'
    ) AS options_read,
    SYSDATE() AS _inserted_timestamp
)
    
SELECT
    VALUE:module::STRING AS dex_id,
    VALUE:name::STRING AS dex,
    VALUE:category::STRING AS category,
    VALUE:chains AS chains,
    _inserted_timestamp
FROM base,
    LATERAL FLATTEN (input=> dex_read:data:protocols)
{% if is_incremental() %}
WHERE dex_id NOT IN (
    SELECT
        DISTINCT dex_id
    FROM
        {{ this }}
)
{% endif %}
UNION
SELECT
    VALUE:module::STRING AS dex_id,
    VALUE:name::STRING AS dex,
    VALUE:category::STRING AS category,
    VALUE:chains AS chains,
    _inserted_timestamp
FROM base,
    LATERAL FLATTEN (input=> options_read:data:protocols)

{% if is_incremental() %}
WHERE dex_id NOT IN (
    SELECT
        DISTINCT dex_id
    FROM
        {{ this }}
)
{% endif %}