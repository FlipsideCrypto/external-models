{{ config(
    materialized = 'view',
    tags = ['streamline_view']
) }}
-- Chains: SCROLL, SUI, FTM, LINEA
-- Address: LINEA, SCROLL, RONIN (Only `newActiveAddresses`), FTM
-- Stats: FTM
WITH chains AS (

    SELECT
        'SCROLL' AS chain_short_name,
        'scroll' AS blockchain
    UNION ALL
    SELECT
        'SUI',
        'sui'
    UNION ALL
    SELECT
        'FTM',
        'fantom'
    UNION ALL
    SELECT
        'LINEA',
        'linea'
    UNION ALL
    SELECT
        'RONIN',
        'ronin'
    UNION ALL
    SELECT
        'ZKSYNC',
        'zksync'
),
metrics AS (
    -- Addresses count metrics (no date params)
    SELECT
        blockchain,
        chain_short_name,
        'address' AS metric,
        'blockchain/address' AS endpoint,
        FALSE AS historical_data,
        'Count of addresses on ' || blockchain AS description,
        NULL :: DATE AS chain_start_date
    FROM
        chains
    UNION ALL
    SELECT
        blockchain,
        chain_short_name,
        'stats' AS metric,
        'blockchain/stats' AS endpoint,
        TRUE AS historical_data,
        'Blockchain statistics for ' || blockchain AS description,
        CASE
            blockchain
            WHEN 'fantom' THEN '2019-12-27'
        END AS chain_start_date
    FROM
        chains
    WHERE
        blockchain IN ('fantom')
)
SELECT
    date_day,
    blockchain,
    metric,
    endpoint,
    chain_short_name,
    (DATE_PART('EPOCH', date_day) * 1000) :: STRING AS xtime,
    description
FROM
    {{ source(
        'crosschain_core',
        'dim_dates'
    ) }}
    CROSS JOIN metrics
WHERE
    (
        metric = 'address'
        AND date_day = SYSDATE() :: DATE
    )
    OR (
        metric = 'stats'
        AND date_day >= chain_start_date
        AND date_day < SYSDATE() :: DATE
    )
