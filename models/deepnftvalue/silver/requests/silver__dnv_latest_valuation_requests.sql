{{ config(
    materialized = 'table',
    enabled = false
) }}

WITH slugs AS (

    SELECT
        collection_slug,
        total_supply
    FROM
        {{ ref('bronze__dnv_collection_slugs') }}
),
offsets AS (
    SELECT
        2500 AS limiter
),
generate_sequence AS (
    SELECT
        SEQ4() AS seq
    FROM
        TABLE(GENERATOR(rowcount => 100000))
),
limits AS (
    SELECT
        *,
        seq * limiter AS offset
    FROM
        generate_sequence
        JOIN offsets
        ON 1 = 1
        JOIN slugs
        ON seq * limiter < total_supply
)
SELECT
    *,
    CASE
        seq
        WHEN 0 THEN CONCAT(
            'https://api.deepnftvalue.com/v1/valuations/',
            collection_slug,
            '?limit=',
            limiter,
            '&token_ids=all'
        )
        ELSE CONCAT(
            'http://api.deepnftvalue.com/v1/valuations/',
            collection_slug,
            '?limit=',
            limiter,
            '&offset=',
            offset,
            '&token_ids=all'
        )
    END AS api_url,
    CONCAT(
        collection_slug,
        '-',
        offset,
        '-',
        SYSDATE() :: DATE
    ) AS _id
FROM
    limits
