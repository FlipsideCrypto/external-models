{{ config(
    materialized = 'incremental',
    cluster_by = 'chain',
    unique_key = 'defillama_usdc_usdt_supply_id',
    tags = ['defillama']
) }}

WITH base AS (

    SELECT
        chain,
        stablecoin_id,
        READ,
        bytes,
        _inserted_timestamp
    FROM
        {{ ref('bronze__defillama_usdc_usdt_supply') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp > (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
flattened_supply AS (
    SELECT
        chain,
        stablecoin_id,
        TO_TIMESTAMP_NTZ(
            VALUE :date :: INT
        ) AS DATE,
        VALUE :totalBridgedToUSD :peggedUSD :: FLOAT AS total_bridged_usd,
        VALUE :totalCirculating :peggedUSD :: FLOAT AS total_circulating,
        VALUE :totalCirculatingUSD :peggedUSD :: FLOAT AS total_circulating_usd,
        _inserted_timestamp
    FROM
        base,
        LATERAL FLATTEN(
            input => READ :data
        )
)
SELECT
    A.date,
    A.stablecoin_id,
    b.stablecoin,
    b.symbol,
    A.chain,
    A.total_bridged_usd,
    A.total_circulating,
    A.total_circulating_usd,
    A._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['chain','date','a.stablecoin_id']
    ) }} AS defillama_usdc_usdt_supply_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flattened_supply A
    LEFT JOIN {{ ref('bronze__defillama_stablecoins') }}
    b
    ON A.stablecoin_id = b.stablecoin_id 
{% if is_incremental() %}
WHERE
    DATE > (
        SELECT
            MAX(DATE) :: DATE
        FROM
            {{ this }}
    )
{% endif %}
    qualify ROW_NUMBER() over (
        PARTITION BY A.chain,
        A.date,
        A.stablecoin_id
        ORDER BY
            A._inserted_timestamp DESC
    ) = 1

