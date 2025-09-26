{{ config(
    materialized = 'incremental',
    unique_key = 'protocol_id',
    tags = ['defillama']
) }}

SELECT
    protocol_id,
    protocol_slug,
    protocol,
    address,
    symbol,
    description,
    chain,
    chains,
    category,
    num_audits,
    audit_note,
    twitter,
    tvl,
    chain_tvls,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['protocol_id']
    ) }} AS dim_protocols_id
FROM {{ ref('silver__defillama_protocols') }}
WHERE 1=1
{% if is_incremental() %}
AND modified_timestamp > (
    SELECT MAX(modified_timestamp) FROM {{ this }}
)
{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_protocols_id ORDER BY modified_timestamp DESC) = 1