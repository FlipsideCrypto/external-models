{{ config(
    materialized = 'incremental',
    unique_key = ['raise_id'],
    tags = ['defillama']
) }}

SELECT 
    funding_date,
    project_name,
    funding_round,
    amount_raised,
    chains,
    sector,
    category,
    category_group,
    source,
    lead_investors,
    other_investors,
    valuation,
    defillama_id,
    raise_id,
    {{ dbt_utils.generate_surrogate_key(
        ['raise_id']
    ) }} AS defillama_raises_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__defillama_raises') }}

{% if is_incremental() %}
WHERE _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}

QUALIFY(
    ROW_NUMBER() OVER (PARTITION BY raise_id ORDER BY _inserted_timestamp DESC)
) = 1