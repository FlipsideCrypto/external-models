{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = ['metric_date'],
    tags = ['aptos_gas'],
) }}

SELECT
    metric_date,
    fund_name,
    workspace_name,
    contact_email,
    fund_balance_remaining,
    total_sponsorships,
    total_fees,
    {{ dbt_utils.generate_surrogate_key([ 'metric_date', 'fund_name','workspace_name']) }} AS shinam_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze_aptos__shinam_data') }}

{% if is_incremental() %}
WHERE
    metric_date >= (
        SELECT
            MAX(metric_date)
        FROM
            {{ this }}
    )
{% endif %}
