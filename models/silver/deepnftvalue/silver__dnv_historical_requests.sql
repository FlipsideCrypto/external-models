{{ config(
    materialized = 'table'
) }}

WITH slugs AS (

    SELECT
        collection_slug,
        total_supply
    FROM
        {{ ref('bronze__dnv_collection_slugs') }}
),
generate_sequence AS (
    SELECT
        SEQ4() AS token_id
    FROM
        TABLE(GENERATOR(rowcount => 20000))
),
all_tokens AS (
    SELECT
        *
    FROM
        generate_sequence
        JOIN slugs
        ON token_id <= total_supply
),
limits AS (
    SELECT
        token_id,
        collection_slug,
        ROW_NUMBER() over (
            PARTITION BY collection_slug
            ORDER BY
                token_id ASC
        ) AS row_no,
        FLOOR((row_no - 1) / 20) AS group_id
    FROM
        all_tokens
    ORDER BY
        token_id ASC
),
format_data AS (
    SELECT
        *,
        MIN(row_no) over (
            PARTITION BY group_id,
            collection_slug
        ) AS min_row_no,
        CASE
            WHEN min_row_no = row_no THEN token_id :: STRING
            ELSE CONCAT(
                '%2C',
                token_id
            )
        END AS format_token_id
    FROM
        limits
),
FINAL AS (
    SELECT
        DISTINCT collection_slug,
        LISTAGG(format_token_id) within GROUP (
            ORDER BY
                token_id
        ) over (
            PARTITION BY collection_slug,
            group_id
        ) AS token_id_list
    FROM
        format_data
)
SELECT
    collection_slug,
    token_id_list,
    CONCAT(
        'https://api.deepnftvalue.com/v1/valuations/hist/',
        collection_slug,
        '?limit=10000&token_ids=',
        token_id_list
    ) AS api_url
FROM
    FINAL
