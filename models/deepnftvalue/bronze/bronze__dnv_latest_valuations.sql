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
        {{ ref('silver__dnv_latest_valuation_requests') }}

{% if is_incremental() %}
WHERE
    _id NOT IN (
        SELECT
            _id
        FROM
            {{ this }}
    )
{% endif %}
),
api_key AS (
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
),
row_nos AS (
    SELECT
        api_url,
        collection_slug,
        _id,
        ROW_NUMBER() over (
            ORDER BY
                api_url
        ) AS row_no,
        FLOOR(
            row_no / 2
        ) + 1 AS batch_no,
        header
    FROM
        requests
        JOIN api_key
        ON 1 = 1
),
batched AS ({% for item in range(11) %}
SELECT
    ethereum.streamline.udf_api(' GET ', api_url, PARSE_JSON(header),{}) AS resp, _id, SYSDATE() _inserted_timestamp, collection_slug
FROM
    row_nos rn
WHERE
    batch_no = {{ item }}
    AND EXISTS (
SELECT
    1
FROM
    row_nos
WHERE
    batch_no = {{ item }}
LIMIT
    1) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %})
SELECT
    resp,
    _inserted_timestamp,
    collection_slug,
    _id
FROM
    batched
