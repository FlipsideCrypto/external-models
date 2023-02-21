{{ config(
    materialized = 'view',
        meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'DEEPNFTVALUE'
            }
        }
    }
) }}

SELECT
    collection_name,
    lower(contract_address) AS collection_address,
    token_id,
    active_offer,
    attributes,
    image,
    is_flagged,
    last_sale,
    owner,
    rarity,
    rarity_rank,
    top_bid,
    valuation,
    _inserted_timestamp AS updated_timestamp
FROM
    {{ ref('silver__dnv_tokens') }}
