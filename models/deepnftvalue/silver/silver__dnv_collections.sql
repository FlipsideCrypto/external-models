{{ config(
    materialized = 'incremental',
    unique_key = '_id',
    tags = ['stale']
) }}

WITH base AS (

    SELECT
        resp,
        _inserted_timestamp
    FROM
        {{ ref('bronze__dnv_collections') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
),
FINAL AS (
    SELECT
        _inserted_timestamp,
        resp :data :attributes AS attributes,
        resp :data :contract_address :: STRING AS collection_address,
        resp :data :floor_price :: FLOAT AS floor_price,
        resp :data :name :: STRING AS collection_name,
        resp :data :slug :: STRING AS slug,
        resp :data :valuation_max :: FLOAT AS valuation_max,
        resp :data :valuation_min :: FLOAT AS valuation_min
    FROM
        base
)
SELECT
    _inserted_timestamp,
    attributes,
    collection_address,
    floor_price,
    collection_name,
    valuation_max,
    valuation_min,
    slug AS _id
FROM
    FINAL
WHERE
    slug IS NOT NULL qualify ROW_NUMBER() over (
        PARTITION BY _id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
