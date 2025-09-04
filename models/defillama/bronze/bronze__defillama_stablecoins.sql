{{ config(
    materialized = 'table',
    tags = ['defillama']
) }}

WITH stablecoin_base AS (

    SELECT
        live.udf_api(
            'GET',
            'https://stablecoins.llama.fi/stablecoins?includePrices=false',{},{}
        ) AS READ,
        SYSDATE() AS _inserted_timestamp
),
FINAL AS (
    SELECT
        VALUE :id :: STRING AS stablecoin_id,
        VALUE :name :: STRING AS stablecoin,
        VALUE :symbol :: STRING AS symbol,
        VALUE :pegType :: STRING AS peg_type,
        VALUE :pegMechanism :: STRING AS peg_mechanism,
        VALUE :priceSource :: STRING AS price_source,
        VALUE :chains AS chains,
        _inserted_timestamp
    FROM
        stablecoin_base,
        LATERAL FLATTEN (
            input => READ :data :peggedAssets
        )
)
SELECT
    stablecoin_id,
    stablecoin,
    symbol,
    peg_type,
    peg_mechanism,
    price_source,
    chains,
    ROW_NUMBER() over (
        ORDER BY
            stablecoin
    ) AS row_num,
    _inserted_timestamp,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    '{{ invocation_id }}' as _invocation_id
FROM
    FINAL