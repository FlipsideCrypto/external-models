{{ config(
    materialized = 'incremental',
    unique_key = '_id',
    full_refresh = false,
    tags = ['stale']
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
        ) - 1 AS batch_no
    FROM
        api_url
),
batched AS ({% for item in range(10) %}
SELECT
    live.udf_api('GET', api_url, OBJECT_CONSTRUCT('Authorization', '{Authorization}', 'accept', 'application/json'),{}, 'Vault/prod/external/deepnftvalue') AS resp, SYSDATE() _inserted_timestamp, collection_slug, CONCAT(collection_slug, '-', _inserted_timestamp) AS _id
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
