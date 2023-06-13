{{ config(
    materialized = 'table',
    enabled = false
) }}

WITH slugs AS (

    SELECT
        collection_slug,
        total_supply,
        created_at,
        5000 AS limiter,
        total_supply / limiter AS total_pages
    FROM
        {{ ref('bronze__dnv_collection_slugs') }}
),
date_series AS (
    SELECT
        date_day
    FROM
        {{ ref('silver__dates') }}
),
generate_sequence AS (
    SELECT
        SEQ4() AS seq
    FROM
        TABLE(GENERATOR(rowcount => 100))
),
all_tokens AS (
    SELECT
        *,
        seq * limiter AS offset
    FROM
        date_series
        JOIN slugs
        ON created_at <= date_day
        AND date_day < SYSDATE() :: DATE
        JOIN generate_sequence
        ON seq <= CEIL(total_pages) - 1
)
SELECT
    collection_slug,
    date_day,
    CONCAT(
        'https://api.deepnftvalue.com/v1/valuations/hist/',
        collection_slug,
        '?limit=',
        limiter,
        '&token_ids=all&start=',
        date_day,
        '&end=',
        date_day
    ) AS api_url1,
    CASE
        WHEN offset = 0 THEN ''
        ELSE CONCAT(
            '&offset=',
            offset
        )
    END AS api_url2,
    CONCAT(
        api_url1,
        api_url2
    ) AS api_url
FROM
    all_tokens
