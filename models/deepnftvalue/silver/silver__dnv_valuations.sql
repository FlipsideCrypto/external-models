{{ config(
    materialized = 'incremental',
    unique_key = '_id'
) }}

WITH historical_base AS (

    SELECT
        resp,
        _inserted_timestamp
    FROM
        {{ ref('bronze__dnv_historical_valuations') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) :: DATE - 1
        FROM
            {{ this }}
    )
{% endif %}
),
latest_base AS (
    SELECT
        resp,
        _inserted_timestamp
    FROM
        {{ ref('bronze__dnv_latest_valuations') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) :: DATE - 1
        FROM
            {{ this }}
    )
{% endif %}
),
all_data AS (
    SELECT
        resp,
        _inserted_timestamp
    FROM
        historical_base
    UNION ALL
    SELECT
        resp,
        _inserted_timestamp
    FROM
        latest_base
),
FINAL AS (
    SELECT
        _inserted_timestamp,
        VALUE :currency :: STRING AS currency,
        VALUE :date :: DATE AS date_day,
        VALUE :nft :collection :name :: STRING AS collection_name,
        VALUE :nft :collection :slug :: STRING AS slug,
        VALUE :nft :token_id :: INTEGER AS token_id,
        VALUE :price :: FLOAT AS price
    FROM
        all_data,
        LATERAL FLATTEN(
            input => resp :data :results
        )
)
SELECT
    _inserted_timestamp,
    currency,
    date_day,
    collection_name,
    slug,
    token_id,
    price,
    contract_address,
    CONCAT(
        contract_address,
        '-',
        token_id,
        '-',
        date_day
    ) AS _id
FROM
    FINAL
    JOIN {{ ref('bronze__dnv_collection_slugs') }}
    b
    ON FINAL.slug = b.collection_slug qualify ROW_NUMBER() over (
        PARTITION BY contract_address,
        token_id,
        date_day
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
