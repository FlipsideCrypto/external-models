{{ config(
    materialized = 'view'
) }}

SELECT
    date_day AS valuation_date,
    collection_name,
    lower(contract_address) AS collection_address,
    token_id,
    currency,
    price,
    _inserted_timestamp AS updated_timestamp
FROM
    {{ ref('silver__dnv_valuations') }}
