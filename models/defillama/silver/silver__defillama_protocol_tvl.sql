-- depends_on: {{ ref('silver__defillama_protocol_tvl_history') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['defillama_tvl_id'],
    cluster_by = ['timestamp', 'protocol_id'],
    tags = ['defillama']
) }}

WITH FINAL AS (

    SELECT
        _inserted_timestamp :: DATE AS TIMESTAMP,
        protocol_id,
        category,
        NAME AS protocol,
        market_cap,
        symbol,
        tvl,
        tvl_prev_day,
        tvl_prev_week,
        tvl_prev_month,
        chain,
        chain_tvl,
        chain_tvl_prev_day,
        chain_tvl_prev_week,
        chain_tvl_prev_month,
        _inserted_timestamp
    FROM
        {{ ref('bronze__defillama_protocol_tvl') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp :: DATE > (
        SELECT
            MAX(
                _inserted_timestamp :: DATE
            )
        FROM
            {{ this }}
    )
{% endif %}
)
{% if is_incremental() and var('HEAL', false) %}
,
historical_heal as(
    SELECT
        timestamp,
        protocol_id,
        category,
        protocol,
        NULL AS market_cap,
        symbol,
        NULL AS tvl,
        NULL AS tvl_prev_day,
        NULL AS tvl_prev_week,
        NULL AS tvl_prev_month,
        chain,
        chain_tvl,
        chain_tvl_prev_day,
        chain_tvl_prev_week,
        chain_tvl_prev_month,
        _inserted_timestamp,
        defillama_protocol_tvl_history_id as defillama_tvl_id,
        inserted_timestamp,
        modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        {{ ref('silver__defillama_protocol_tvl_history') }}
    WHERE
        defillama_protocol_tvl_history_id not in (
            select 
                distinct defillama_tvl_id 
            FROM
                {{ this }}
        )
)
{% endif %}
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['protocol_id','chain','timestamp']
    ) }} AS defillama_tvl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
{% if is_incremental() and var('HEAL', false) %}
UNION ALL
SELECT
    *
FROM
    historical_heal
{% endif %}