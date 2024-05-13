{{ config(
    materialized = 'incremental',
    unique_key = 'defillama_historical_yields_id',
    tags = ['defillama','200']
) }}

WITH 
{# upstream_complete_check as (
    select
        'check'
    FROM
        {{ ref('bronze__defillama_historical_yields_101_200') }}
), #}

historical_yield AS (

{% for item in range(100) %}
(
SELECT
    pool_id,
    symbol,
    live.udf_api(
        'GET',concat('https://yields.llama.fi/chart/',pool_id),{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp,
FROM (
    SELECT 
        pool_id,
        symbol,
        row_num
    FROM {{ ref('silver__defillama_yields') }}
    WHERE row_num BETWEEN {{ ((item + 100) * 33)+1 }} AND {{ (item + 101) * 33 }}
    )
    {% if is_incremental() %}
    WHERE pool_id NOT IN (
    SELECT
        pool_id
    FROM (
        SELECT 
            DISTINCT pool_id,
            MAX(_inserted_timestamp::DATE) AS max_timestamp
        FROM {{ this }}
        GROUP BY 1
        HAVING CURRENT_DATE = max_timestamp
    ))
{% endif %}
){% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
),
flatten AS (
    select 
        pool_id,
        symbol,
        value:apy::float as apy,
        value:apyBase::float as apy_base,
        value:apyBase7d::float as apy_base_7d,
        value:apyReward::float as apy_reward,
        value:il7d::float as il_7d,
        value:timestamp::date as date,
        value:tvlUsd::float as tvl_usd,
        _inserted_timestamp
    FROM historical_yield,
        LATERAL FLATTEN (input=> read:data:data)
),
FINAL AS (
    select
        date,
        pool_id,
        symbol,
        apy,
        apy_base,
        apy_base_7d,
        apy_reward,
        il_7d,
        tvl_usd,
        _inserted_timestamp
    FROM
        flatten
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['pool_id','date']
    ) }} AS defillama_historical_yields_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
