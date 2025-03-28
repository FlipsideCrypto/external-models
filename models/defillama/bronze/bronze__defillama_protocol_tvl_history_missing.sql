{{ config(
    materialized = 'incremental',
    full_refresh = false,
    unique_key = ['protocol_id','chain','timestamp'],
    cluster_by = ['chain'],
    tags = ['defillama']
) }}

WITH dates AS (

    SELECT
        DATEADD('day', SEQ4(), CURRENT_DATE - 120) AS DATE
    FROM
        TABLE(GENERATOR(rowcount => 120))
    WHERE
        DATE <= CURRENT_DATE
),
protocols AS (
    SELECT
        DISTINCT NAME
    FROM
        EXTERNAL.bronze.defillama_protocol_tvl
),
actual_dates AS (
    SELECT
        TIMESTAMP :: DATE AS DATE,
        NAME,
        COUNT(*) AS record_count,
        SUM(tvl) AS total_tvl
    FROM
        EXTERNAL.bronze.defillama_protocol_tvl
    GROUP BY
        1,
        2
)
SELECT
    p.name AS protocol_name,
    d.date AS missing_date
FROM
    dates d
    CROSS JOIN protocols p
    LEFT JOIN actual_dates A
    ON d.date = A.date
    AND p.name = A.name
WHERE
    A.date IS NULL
ORDER BY
    p.name,
    d.date DESC;
