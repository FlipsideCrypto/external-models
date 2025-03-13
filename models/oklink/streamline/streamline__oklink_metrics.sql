{{
    config(
        materialized = 'view',
        tags = ['streamline_view']
    )
}}

-- Chains: SCROLL, SUI, FTM, LINEA, RONIN (Flipside Supported)
-- Address: LINEA, SCROLL, RONIN (Only `newActiveAddresses`), FTM
-- Stats: FTM

WITH metrics AS (
    -- Addresses count metrics (no date params)
    SELECT
        'Linea' AS blockchain,
        'LINEA' AS chain_short_name,
        'address_count' AS metric,
        'https://www.oklink.com/api/v5/explorer/blockchain/address' AS endpoint,
        FALSE AS historical_data,
        'Count of addresses on Linea blockchain' AS description
    UNION ALL
    SELECT
        'Scroll' AS blockchain,
        'SCROLL' AS chain_short_name,
        'address_count' AS metric,
        'https://www.oklink.com/api/v5/explorer/blockchain/address' AS endpoint,
        FALSE AS historical_data,
        'Count of addresses on Scroll blockchain' AS description
    UNION ALL
    -- Only `activeAddresses`/`newActiveAddresses` available for Ronin
    SELECT
        'Ronin' AS blockchain,
        'RONIN' AS chain_short_name,
        'address_count' AS metric,
        'https://www.oklink.com/api/v5/explorer/blockchain/address' AS endpoint,
        FALSE AS historical_data,
        'Count of addresses on Ronin blockchain' AS description
    UNION ALL
    SELECT
        'Fantom' AS blockchain,
        'FTM' AS chain_short_name,
        'address_count' AS metric,
        'https://www.oklink.com/api/v5/explorer/blockchain/address' AS endpoint,
        FALSE AS historical_data,
        'Count of addresses on Ronin blockchain' AS description
    UNION ALL
    -- Blockchain stats metrics (can use StartTime and endTime)
        SELECT
        'Fantom' AS blockchain, 
        'FTM' AS chain_short_name,
        'blockchain_stats' AS metric,
        'https://www.oklink.com/api/v5/explorer/blockchain/stats' AS endpoint,
        TRUE AS historical_data,
        'Blockchain statistics for Fantom' AS description
)

SELECT
    date_day,
    blockchain,
    metric,
    endpoint,
    CASE
        WHEN metric IN ('address_count') THEN
            -- For endpoints without look-back functionality (i.e. address)
            OBJECT_CONSTRUCT(
                'chainShortName', chain_short_name
            )
        ELSE
            -- For endpoints with look-back functionality (i.e. stats)
            OBJECT_CONSTRUCT(
                'chainShortName', chain_short_name,
                'startTime', ((DATE_PART('EPOCH', date_day)) * 1000)::STRING,
                'endTime', ((DATE_PART('EPOCH', DATEADD(SECOND, -1, DATEADD(DAY, 1, date_day)))) * 1000)::STRING,
                'limit', '1',
                'page', '1'
            )
    END AS variables,
    description
FROM
    {{ source(
        'crosschain_core',
        'dim_dates'
    ) }}
    CROSS JOIN metrics
WHERE
    (metric = 'address_count' AND date_day = SYSDATE())
    OR 
    (metric = 'blockchain_stats' AND date_day >= '2025-01-01' AND date_day < SYSDATE())
