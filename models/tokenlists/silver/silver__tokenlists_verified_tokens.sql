{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'tokenlists_verified_tokens_id',
    tags = ['tokenlists']
) }}

WITH base_lists AS (

    SELECT
        api_url,
        request,
        keywords,
        logo_uri,
        list_name,
        list_tags,
        VALUE :name :: STRING AS NAME,
        VALUE :symbol :: STRING AS symbol,
        VALUE :address :: STRING AS address,
        VALUE :chainId :: STRING AS chain_id,
        VALUE :decimals :: STRING AS decimals,
        VALUE :extensions :: VARIANT AS extensions,
        _inserted_timestamp,
        all_tokenlists_id
    FROM
        {{ ref('bronze__all_tokenlists') }},
        LATERAL FLATTEN (
            input => request :data :tokens
        )

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    api_url,
    NAME,
    symbol,
    address,
    chain_id,
    decimals,
    extensions,
    list_name AS provider,
    list_tags AS list_metadata,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['api_url', 'LOWER(address)', 'chain_id']
    ) }} AS tokenlists_verified_tokens_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    base_lists
WHERE
    address IS NOT NULL

{% if is_incremental() %}
AND api_url || LOWER(address) || chain_id NOT IN (
    SELECT
        api_url || LOWER(address) || chain_id
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY api_url, LOWER(address), chain_id
ORDER BY
    _inserted_timestamp DESC)) = 1
