{{ config(
    materialized = 'table'
) }}

WITH base AS (

    SELECT
        collection_address,
        collection_name,
        token_id,
        price,
        valuation_date,
        currency,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_valuations') }}
)
SELECT
    valuation_date,
    collection_name,
    LOWER(collection_address) AS collection_address,
    token_id,
    currency,
    price,
    _inserted_timestamp,
    CONCAT(
        collection_name,
        '-',
        token_id,
        '-',
        valuation_date
    ) AS _id
FROM
    base
