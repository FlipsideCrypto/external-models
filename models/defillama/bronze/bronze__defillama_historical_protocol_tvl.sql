{{ config(
    materialized = 'incremental',
    unique_key = 'defillama_historical_protocol_tvl'
) }}

WITH 

protocol_tvl AS (

{% for item in range(14) %}
(
SELECT
    protocol_id,
    protocol,
    protocol_slug,
    live.udf_api(
        'GET',concat('https://api.llama.fi/protocol/',protocol_slug),{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp,
FROM (
    SELECT 
        protocol_id,
        protocol,
        protocol_slug,
        row_num
    FROM {{ ref('bronze__defillama_protocols') }}
    WHERE row_num BETWEEN {{ item * 10 + 1 }} AND {{ (item + 1) * 10 }}
    )
    {% if is_incremental() %}
    WHERE protocol_slug NOT IN (
    SELECT
        protocol_slug
    FROM (
        SELECT 
            DISTINCT protocol_slug,
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
        protocol_id,
        protocol,
        protocol_slug,
        to_timestamp(value:date) as date,
        value:totalLiquidityUSD::float as tvl,
        _inserted_timestamp
    FROM protocol_tvl,
        LATERAL FLATTEN (input=> read:data:tvl)
),
FINAL AS (
    select
        protocol_id,
        protocol,
        protocol_slug,
        date,
        tvl,
        _inserted_timestamp
    FROM
        flatten
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['protocol_id','date']
    ) }} AS defillama_historical_protocol_tvl,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
