{{ config(
    materialized = 'table',
    unique_key = ['model','unfilled_row_count'],
    tags = ['defillama']
) }}

WITH pools AS (

    SELECT
        DISTINCT pool_id,
        MAX(
            _inserted_timestamp :: DATE
        ) AS max_timestamp
    FROM
        {{ ref('silver__defillama_historical_yields') }}
    GROUP BY
        1
    HAVING
        CURRENT_DATE = max_timestamp
),
max_row AS (
    SELECT
        MAX(row_num) AS max_row
    FROM
        {{ ref('silver__defillama_yields') }}
),
backfill_pools AS (
    SELECT
        pool_id,
        symbol,
        row_num,
        CASE
            WHEN row_num > 0
            AND row_num < 3301 THEN '100'
            WHEN row_num >= 3301
            AND row_num < 6601 THEN '200'
            WHEN row_num >= 6601
            AND row_num < 9901 THEN '300'
            WHEN row_num >= 9901
            AND row_num < 13200 THEN '400'
            WHEN row_num >= 13200 THEN '500'
        END AS model
    FROM
        external_DEV.silver.defillama_yields
    WHERE
        row_num BETWEEN 1
        AND (
            SELECT
                max_row
            FROM
                max_row
        )
        AND pool_id NOT IN (
            SELECT
                pool_id
            FROM
                pools
        )
),
groupings AS (
    SELECT
        '100' AS model,
        1 AS min_row,
        3300 AS max_row
    UNION ALL
    SELECT
        '200' AS model,
        3301 AS min_row,
        6600 AS max_row
    UNION ALL
    SELECT
        '300' AS model,
        6601 AS min_row,
        9900 AS max_row
    UNION ALL
    SELECT
        '400' AS model,
        9901 AS min_row,
        12000 AS max_row
    UNION ALL
    SELECT
        '500' AS model,
        12001 AS min_row,
        (
            SELECT
                MAX(row_num)
            FROM
                {{ ref('silver__defillama_yields') }}
        ) AS max_row
)
SELECT
    g.model,
    COUNT(*) AS unfilled_row_count
FROM
    backfill_pools g
    LEFT JOIN groupings p
    ON g.model = p.model
GROUP BY
    1
