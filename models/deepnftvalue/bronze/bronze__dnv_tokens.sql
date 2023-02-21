{{ config(
    materialized = 'incremental',
    unique_key = '_id',
    full_refresh = false
) }}

WITH requests AS (

    SELECT
        api_url,
        collection_slug,
        _id
    FROM
        {{ ref('silver__dnv_token_requests') }}

{% if is_incremental() %}
WHERE
    _id NOT IN (
        SELECT
            _id
        FROM
            {{ this }}
    )
{% endif %}
ORDER BY
    collection_slug
LIMIT
    2
), api_key AS (
    SELECT
        CONCAT(
            '{\'Authorization\': \'Token ',
            api_key,
            '\', \'accept\': \'application/json\'}'
        ) AS header
    FROM
        {{ source(
            'crosschain_silver',
            'apis_keys'
        ) }}
    WHERE
        api_name = 'deepnftvalue'
)
SELECT
    ethereum.streamline.udf_api(' GET ', api_url, PARSE_JSON(header),{}) AS resp,
    SYSDATE() _inserted_timestamp,
    collection_slug,
    _id
FROM
    requests
    JOIN api_key
    ON 1 = 1
WHERE
    EXISTS (
        SELECT
            1
        FROM
            requests
    )
