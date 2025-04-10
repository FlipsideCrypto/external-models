{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'DEEPNFTVALUE' } } },
    tags = ['stale']
) }}

SELECT
    valuation_date,
    collection_name,
    collection_address,
    token_id,
    currency,
    price,
    _inserted_timestamp AS updated_timestamp
FROM
    {{ ref('silver__dnv_valuations') }}
