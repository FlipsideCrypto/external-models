{{ config(
    materialized = 'view'
) }}

SELECT
    date_day AS valuation_day,
    collection_name,
    token_id,
    currency,
    price,
    _inserted_timestamp AS collected_timestamp
FROM
    {{ ref('silver__dnv_historical_valuations') }}
