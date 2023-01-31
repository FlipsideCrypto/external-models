{{ config(
    materialized = 'table'
) }}

WITH slugs AS (

    SELECT
        collection_slug
    FROM
        {{ ref('bronze__dnv_collection_slugs') }}
),
api_endpoint AS (
    SELECT
        'https://api.deepnftvalue.com/v1/valuations/hist/' AS api_endpoint
),
api_limits AS (
    SELECT
        '?limit=5&token_ids=10' AS api_limits
),
api_url AS (
    SELECT
        api_endpoint || collection_slug || api_limits AS api_url
    FROM
        slugs
        CROSS JOIN api_endpoint
        CROSS JOIN api_limits
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
)
SELECT
    ethereum.streamline.udf_api(' GET ', api_url, PARSE_JSON(header),{}) AS resp,
    SYSDATE() _inserted_timestamp
FROM
    api_url
    CROSS JOIN api_key
