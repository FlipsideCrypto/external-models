{{ config(
    materialized = 'incremental',
    unique_key = '_id'
) }}

WITH base AS (

    SELECT
        resp,
        _inserted_timestamp,
        collection_slug AS slug
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
),
FINAL AS (
    SELECT
        _inserted_timestamp,
        slug,
        VALUE :token_id AS token_id,
        VALUE :active_offer AS active_offer,
        VALUE :attributes AS attributes,
        VALUE :attributes_synthetic AS attributes_synthetic,
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
)
SELECT
    token_id,
    active_offer,
    attributes,
    attributes_synthetic,
    collection :contract :: STRING AS contract_address,
    collection :name :: STRING AS collection_name,
    image,
    is_flagged,
    last_sale,
    owner,
    rarity,
    rarity_rank,
    top_bid,
    valuation,
    _inserted_timestamp,
    CONCAT(
        slug,
        '-',
        token_id
    ) AS _id
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY _id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
