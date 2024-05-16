{{ config(
    materialized = 'incremental',
    unique_key = 'stablecoin_id',
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

{% if is_incremental() %}
WHERE
    stablecoin_id NOT IN (
        SELECT
            DISTINCT stablecoin_id
        FROM
            {{ this }}
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
    m.row_num + ROW_NUMBER() over (
        ORDER BY
            stablecoin
    ) AS row_num,
    _inserted_timestamp
FROM
    FINAL
    JOIN (
        SELECT
            MAX(row_num) AS row_num
        FROM
            {{ this }}
    ) m
    ON 1 = 1
{% else %}
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
    _inserted_timestamp
FROM
    FINAL
{% endif %}
