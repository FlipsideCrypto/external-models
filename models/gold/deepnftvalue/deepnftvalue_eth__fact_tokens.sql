{{ config(
    materialized = 'view'
) }}

SELECT
    token_id,
    active_offer,
    attributes,
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
    _inserted_timestamp AS collected_timestamp
FROM
    {{ ref('silver__dnv_tokens') }}
