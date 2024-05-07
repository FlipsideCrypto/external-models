{{ config(
    materialized = 'incremental',
    unique_key = ['defillama_tvl_id'],
    cluster_by = ['chain']
) }}

with FINAL AS (
    SELECT
        
        protocol_id,
        category,
        NAME,
        market_cap,
        symbol,
        _inserted_timestamp as timestamp,
        tvl,
        tvl_prev_day,
        tvl_prev_week,
        tvl_prev_month,
        chain,
        chain_tvl,
        chain_tvl_prev_day,
        chain_tvl_prev_week,
        chain_tvl_prev_month,
        _inserted_timestamp
    FROM
        {{ ref('bronze__defillama_protocols_tvl') }}
    WHERE
        chain IN (
            'Ethereum',
            'BSC',
            'Arbitrum',
            'Polygon',
            'Avalanche',
            'Base',
            'Optimism',
            'Solana',
            'Kava',
            'Cronos',
            'Blast',
            'zkSync Era',
            'Linea',
            'Mantle',
            'Scroll',
            'Gnosis',
            'Polygon zkEVM',
            'Aurora',
            'Moonbeam',
            'Harmony',
            'Metis',
            'Moonriver',
            'Klaytn',
            'Heco',
            'Celo',
            'Manta',
            'Dogechain'
        )
    AND
        chain_tvl > 1000
    {% if is_incremental() %}
    AND
        _inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                ) - INTERVAL '36 hours'
            FROM
                {{ this }}
        )
    {% endif %}
    
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['protocol_id','chain','_inserted_timestamp']
    ) }} AS defillama_tvl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
