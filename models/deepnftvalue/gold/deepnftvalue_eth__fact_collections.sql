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
    lower(collection_address) AS collection_address,
    floor_price,
    valuation_max,
    valuation_min,
    attributes,
    _inserted_timestamp AS valuation_timestamp
FROM
    {{ ref('silver__dnv_collections') }}
