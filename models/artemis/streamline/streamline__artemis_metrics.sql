{{ config(
    materialized = 'view',
    tags = ['streamline_view']
) }}

-- Chains: Scroll, Sui, Stacks, Cardano, Celo, Linea, MultiversX, Starknet, Immutable, Celestia, Wormhole, Acala, Astar, zkSync, Gnosis, Metis, Centrifuge, Fantom, Sonic, Moonbeam, Unichain, Algorand, Berachain
-- Metrics: DAILY_TXNS (tx_count), DAU (active_users), DAU Cumulative for (TAM? - might introduce double counting since unique wallets are counted daily)

WITH chains AS (

    SELECT
        'scroll' AS artemis_id
    UNION ALL
    SELECT
        'sui'
    UNION ALL
    SELECT
        'stacks'
    UNION ALL
    SELECT
        'cardano'
    UNION ALL
    SELECT
        'celo'
    UNION ALL
    SELECT
        'linea'
    UNION ALL
    SELECT
        'multiversx'
    UNION ALL
    SELECT
        'starknet'
    UNION ALL
    SELECT
        'immutable_x'
    UNION ALL
    SELECT
        'celestia'
    UNION ALL
    SELECT
        'wormhole'
    UNION ALL
    SELECT
        'acala'
    UNION ALL
    SELECT
        'astar'
    UNION ALL
    SELECT
        'zksync'
    UNION ALL
    SELECT
        'gnosis'
    UNION ALL
    SELECT
        'metis'
    UNION ALL
    SELECT
        'centrifuge'
    UNION ALL
    SELECT
        'fantom'
    UNION ALL
    SELECT
        'sonic'
    UNION ALL
    SELECT
        'moonbeam'
    UNION ALL
    SELECT
        'unichain'
    UNION ALL
    SELECT
        'algorand'
    UNION ALL
    SELECT
        'berachain'
),
metrics AS (
    SELECT
        artemis_id AS blockchain,
        'tx_count' AS metric,
        '{Service}/data/' AS url,
        'daily_txns' AS endpoint,
        'Daily transaction count' AS description
    FROM
        chains
    UNION ALL
    SELECT
        artemis_id AS blockchain,
        'active_users' AS metric,
        '{Service}/data/' AS url,
        'dau' AS endpoint,
        'Daily active users count' AS description
    FROM
        chains
)
SELECT
    date_day,
    blockchain,
    metric,
    endpoint,
    url,
    description
FROM
    {{ source(
        'crosschain_core',
        'dim_dates'
    ) }}
    CROSS JOIN metrics
WHERE date_day >= '2025-01-01'