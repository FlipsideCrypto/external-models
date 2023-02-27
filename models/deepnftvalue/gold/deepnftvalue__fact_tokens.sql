{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'DEEPNFTVALUE' }} }
) }}

SELECT
    collection_name,
    LOWER(contract_address) AS collection_address,
    token_id :: INTEGER AS token_id,
    active_offer AS active_offer,
    attributes,
    image,
    CASE
        WHEN is_flagged :: STRING = 'true' THEN TRUE
        ELSE FALSE
    END is_flagged,
    last_sale :: OBJECT AS last_sale,
    owner :: OBJECT AS owner,
    rarity :: FLOAT AS rarity,
    rarity_rank :: INTEGER AS rarity_rank,
    top_bid :: OBJECT AS top_bid,
    valuation :: OBJECT AS valuation,
    _inserted_timestamp AS updated_timestamp
FROM
    {{ ref('silver__dnv_tokens') }}
