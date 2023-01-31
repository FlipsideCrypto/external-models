{{ config(
    materialized = 'table'
) }}

WITH base AS (

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
            )
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    _inserted_timestamp,
    VALUE :currency :: STRING AS currency,
    VALUE :date :: DATE AS date_day,
    VALUE :nft :collection :name :: STRING AS collection_name,
    VALUE :nft :collection :slug :: STRING AS slug,
    VALUE :nft :token_id :: INTEGER AS token_id,
    VALUE :price :: FLOAT AS price
FROM
    base,
    LATERAL FLATTEN(
        input => resp :data :results
    )
