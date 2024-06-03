{{ config(
    materialized = 'incremental',
    unique_key = 'defillama_yield_id',
    tags = ['defillama']
) }}

WITH pool_yields AS (

    SELECT
        live.udf_api('GET', CONCAT('https://yields.llama.fi/pools'),{},{}) AS READ,
        SYSDATE() :: DATE AS TIMESTAMP,
        SYSDATE() AS _inserted_timestamp
),
FINAL AS (
    SELECT
        TIMESTAMP,
        VALUE :apy :: FLOAT AS apy,
        VALUE :apyBase :: FLOAT AS apy_base,
        VALUE :apyBase7d :: FLOAT AS base_7d,
        VALUE :apyBaseInception :: FLOAT AS apy_base_inception,
        VALUE :apyMean30d :: FLOAT AS apy_mean_30d,
        VALUE :apyPct1D :: FLOAT AS apy_pct_1d,
        VALUE :apyPct30D :: FLOAT AS apy_pct_30d,
        VALUE :apyPct7D :: FLOAT AS apy_pct_7d,
        VALUE :apyReward :: FLOAT AS apy_rewards,
        VALUE :chain :: STRING AS chain,
        VALUE :count :: INTEGER AS COUNT,
        VALUE :exposure :: STRING AS exposure,
        VALUE :il7d :: FLOAT AS il_7d,
        VALUE :ilRisk :: STRING AS il_risk,
        VALUE :mu :: FLOAT AS mu,
        VALUE :outlier :: BOOLEAN AS outlier,
        VALUE :pool :: STRING AS pool_id,
        VALUE :poolMeta :: STRING AS pool_meta,
        VALUE :predictions :: variant AS predictions,
        VALUE :project :: STRING AS projects,
        VALUE :rewardTokens :: variant AS reward_tokens,
        VALUE :sigma :: FLOAT AS sigma,
        VALUE :stablecoin :: BOOLEAN AS stablecoin,
        VALUE :symbol :: STRING AS symbol,
        VALUE :tvlUsd :: INTEGER AS tvl_usd,
        VALUE :underlyingTokens :: variant AS underlying_tokens,
        VALUE :volumeUsd1d :: FLOAT AS volume_usd_1d,
        VALUE :volumeUsd7d :: FLOAT AS volume_usd_7d,
        _inserted_timestamp
    FROM
        pool_yields,
        LATERAL FLATTEN (
            input => READ :data :data
        )

{% if is_incremental() %}
WHERE
    _inserted_timestamp :: DATE > (
        SELECT
            MAX(_inserted_timestamp) :: DATE
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    TIMESTAMP,
    apy,
    apy_base,
    base_7d,
    apy_base_inception,
    apy_mean_30d,
    apy_pct_1d,
    apy_pct_30d,
    apy_pct_7d,
    apy_rewards,
    chain,
    COUNT,
    exposure,
    il_7d,
    il_risk,
    mu,
    outlier,
    pool_id,
    pool_meta,
    predictions,
    projects,
    CASE
        WHEN reward_tokens [0] IS NULL THEN NULL
        ELSE reward_tokens
    END AS reward_tokens,
    sigma,
    stablecoin,
    symbol,
    tvl_usd,
    CASE
        WHEN underlying_tokens [0] IS NULL THEN NULL
        ELSE underlying_tokens
    END AS underlying_tokens,
    volume_usd_1d,
    volume_usd_7d,
    {{ dbt_utils.generate_surrogate_key(
        ['pool_id','chain','timestamp']
    ) }} AS defillama_yield_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
