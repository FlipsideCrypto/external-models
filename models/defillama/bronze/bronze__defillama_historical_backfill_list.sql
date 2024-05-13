{{ config(
    materialized = 'table',
    unique_key = 'defillama_historical_yields_id',
    tags = ['defillama']
) }}

WITH pools as (
    SELECT 
        DISTINCT pool_id,
            MAX(_inserted_timestamp::DATE) AS max_timestamp
    FROM 
        {{ ref('silver__defillama_historical_yields') }}
    GROUP BY 1
    HAVING CURRENT_DATE = max_timestamp
),
max_row as (
    select
        max(row_num) as max_row
    from
        {{ ref('silver__defillama_yields') }}
),
backfill_pools as (
    SELECT 
        pool_id,
        symbol,
        row_num
    FROM external_DEV.silver.defillama_yields
    WHERE row_num BETWEEN 1 AND (select max_row from max_row)
    AND POOL_ID NOT IN (select pool_id from pools)
),
groupings as (
    SELECT
        '100' as model,
        1 as min_row,
        3300 as max_row
    UNION ALL
    SELECT
        '200' as model,
        3301 as min_row,
        6600 as max_row
    UNION ALL
    SELECT
        '300' as model,
        6601 as min_row,
        9900 as max_row
    UNION ALL
    SELECT
        '400' as model,
        9901 as min_row,
        12000 as max_row
    UNION ALL
    SELECT
        '500' as model,
        12001 as min_row,
        15000 as max_row
)
SELECT
    model
FROM    
    groupings
WHERE
    max_row >(select min(row_num) from backfill_pools)