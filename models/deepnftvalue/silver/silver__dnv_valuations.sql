-- depends_on: {{ ref('bronze__streamline_valuations') }}
{{ config(
    materialized = 'incremental',
    unique_key = "_id",
    incremental_strategy = "delete+insert",
    cluster_by = ['valuation_date::DATE'],
    tags = ['stale']
) }}

WITH base AS (

    SELECT
        collection_address,
        collection_name,
        token_id,
        price,
        valuation_date,
        currency,
        _inserted_timestamp,
        collection_slug
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_valuations') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) :: DATE
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_fr_valuations') }}
{% endif %}
)
SELECT
    valuation_date,
    collection_name,
    LOWER(collection_address) AS collection_address,
    token_id,
    currency,
    price,
    collection_slug,
    _inserted_timestamp,
    CONCAT(
        collection_slug,
        '-',
        token_id,
        '-',
        valuation_date
    ) AS _id
FROM
    base qualify ROW_NUMBER() over (
        PARTITION BY _id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
