{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'DEEPNFTVALUE' }}}
) }}

SELECT
    date_day AS valuation_date,
    collection_name,
    LOWER(contract_address) AS collection_address,
    token_id,
    currency,
    price,
    _inserted_timestamp AS updated_timestamp
FROM
    {{ ref('silver__dnv_valuations') }}
