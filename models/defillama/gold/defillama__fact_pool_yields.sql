{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['defillama'],
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'DEFILLAMA'
            }
        }
    }
) }}

SELECT
    TIMESTAMP :: DATE AS DATE,
    pool_id,
    projects,
    chain,
    symbol,
    apy,
    apy_base,
    base_7d,
    apy_base_inception,
    apy_mean_30d,
    apy_pct_1d,
    apy_pct_7d,
    apy_pct_30d,
    apy_rewards,
    il_7d,
    il_risk,
    reward_tokens,
    mu,
    sigma,
    stablecoin,
    tvl_usd,
    underlying_tokens,
    volume_usd_1d,
    volume_usd_7d
FROM
    {{ ref('silver__defillama_pool_yields') }} f
