{{ config(
    materialized = 'incremental',
    unique_key = '_id',
    full_refresh = false
) }}

WITH slugs AS (

    SELECT
        collection_slug
    FROM
        {{ ref('bronze__dnv_collection_slugs') }}
),
api_endpoint AS (
    SELECT
        'https://api.deepnftvalue.com/v1/collections/' AS api_endpoint
),
api_url AS (
    SELECT
        api_endpoint || collection_slug AS api_url,
        collection_slug
    FROM
        slugs
        CROSS JOIN api_endpoint
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
        ROW_NUMBER() over (
            ORDER BY
                api_url
        ) AS row_no,
        FLOOR(
            row_no / 1
        ) - 1 AS batch_no,
        header
    FROM
        api_url
        CROSS JOIN api_key
),
batched AS ({% for item in range(10) %}
SELECT
    ethereum.streamline.udf_api(' GET ', api_url, PARSE_JSON(header),{}) AS resp, SYSDATE() _inserted_timestamp, collection_slug, CONCAT(collection_slug, '-', _inserted_timestamp) AS _id
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
    _id
FROM
    batched
