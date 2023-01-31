{{ config(
    materialized = 'table'
) }}

WITH base AS (

    SELECT
        resp,
        _inserted_timestamp
    FROM
        {{ ref('bronze__dnv_tokens') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    _inserted_timestamp,
    VALUE :token_id AS token_id,
    VALUE :active_offer AS active_offer,
    VALUE :attributes AS attributes,
    VALUE :collection AS collection,
    VALUE :image AS image,
    VALUE :is_flagged AS is_flagged,
    VALUE :last_sale AS last_sale,
    VALUE :owner AS owner,
    VALUE :rarity AS rarity,
    VALUE :rarity_rank AS rarity_rank,
    VALUE :top_bid AS top_bid,
    VALUE :valuation AS valuation
FROM
    base,
    LATERAL FLATTEN(
        input => resp :data :results
    )
