{{ config (
    materialized = 'view'
) }}

SELECT
    collection_address,
    collection_name,
    token_id,
    price,
    valuation_date,
    currency,
    CURRENT_TIMESTAMP :: timestamp_ntz AS _inserted_timestamp
FROM
    {{ source(
        'bronze_streamline',
        'valuations'
    ) }}
