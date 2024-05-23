{{ config(
    materialized = 'incremental',
    unique_key = ['defillama_tvl_id'],
    cluster_by = ['chain'],
    tags = ['defillama']
) }}

with FINAL AS (
    SELECT
        SYSDATE()::DATE as TIMESTAMP,
        protocol_id,
        category,
        NAME as protocol,
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
        _inserted_timestamp::DATE > (
            SELECT
                MAX(
                    _inserted_timestamp ::DATE
                )
            FROM
                {{ this }}
        )
    {% endif %}
    
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['protocol_id','chain','_inserted_timestamp']
    ) }} AS defillama_tvl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
