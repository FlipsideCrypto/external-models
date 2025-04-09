{{ config(
    materialized = 'incremental',
    unique_key = 'defillama_usdc_usdt_supply_id',
    tags = ['defillama']
) }}

WITH usdc_supply AS (

    SELECT
        *
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
        usdc_supply,
        LATERAL FLATTEN(
            input => READ :data
        )

)
SELECT
    a.date,
    a.stablecoin_id,
    b.stablecoin,
    b.symbol,
    a.chain,
    a.total_bridged_usd,
    a.total_circulating,
    a.total_circulating_usd,
    a._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['chain','date','a.stablecoin_id']
    ) }} AS defillama_usdc_usdt_supply_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flattened_supply a
LEFT JOIN
    {{ ref('bronze__defillama_stablecoins') }} b
ON
    a.stablecoin_id = b.stablecoin_id

{% if is_incremental() %}
WHERE
    date > (
        SELECT
            MAX(date) :: DATE
        FROM
            {{ this }}
    )
{% endif %}
