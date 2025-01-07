{{ config(
    materialized = 'table',
    enabled = false,
    unique_key = 'defillama_stablecoin_supply_id',
    tags = ['defillama']
) }}

WITH expand_flatten AS (

    SELECT
        f.address,
        f.symbol,
        f.name,
        f.stablecoin_id,
        d.chains,
        d.peg_type AS peg_type,
        CASE
            WHEN d.peg_type = 'peggedUSD' THEN VALUE :circulating :peggedUSD :: INTEGER
            WHEN d.peg_type = 'peggedVAR' THEN VALUE :circulating :peggedVAR :: INTEGER
            WHEN d.peg_type = 'peggedJPY' THEN VALUE :circulating :peggedJPY :: INTEGER
            WHEN d.peg_type = 'peggedEUR' THEN VALUE :circulating :peggedEUR :: INTEGER
            WHEN d.peg_type = 'peggedAUD' THEN VALUE :circulating :peggedAUD :: INTEGER
            WHEN d.peg_type = 'peggedCAD' THEN VALUE :circulating :peggedCAD :: INTEGER
            WHEN d.peg_type = 'peggedGBP' THEN VALUE :circulating :peggedGBP :: INTEGER
            WHEN d.peg_type = 'peggedCNY' THEN VALUE :circulating :peggedCNY :: INTEGER
            WHEN d.peg_type = 'peggedUAH' THEN VALUE :circulating :peggedUAH :: INTEGER
            WHEN d.peg_type = 'peggedCHF' THEN VALUE :circulating :peggedCHF :: INTEGER
            WHEN d.peg_type = 'peggedARS' THEN VALUE :circulating :peggedARS :: INTEGER
        END AS circulating,
        CASE
            WHEN d.peg_type = 'peggedUSD' THEN VALUE :minted :peggedUSD :: INTEGER
            WHEN d.peg_type = 'peggedVAR' THEN VALUE :minted :peggedVAR :: INTEGER
            WHEN d.peg_type = 'peggedJPY' THEN VALUE :minted :peggedJPY :: INTEGER
            WHEN d.peg_type = 'peggedEUR' THEN VALUE :minted :peggedEUR :: INTEGER
            WHEN d.peg_type = 'peggedAUD' THEN VALUE :minted :peggedAUD :: INTEGER
            WHEN d.peg_type = 'peggedCAD' THEN VALUE :minted :peggedCAD :: INTEGER
            WHEN d.peg_type = 'peggedGBP' THEN VALUE :minted :peggedGBP :: INTEGER
            WHEN d.peg_type = 'peggedCNY' THEN VALUE :minted :peggedCNY :: INTEGER
            WHEN d.peg_type = 'peggedUAH' THEN VALUE :minted :peggedUAH :: INTEGER
            WHEN d.peg_type = 'peggedCHF' THEN VALUE :minted :peggedCHF :: INTEGER
            WHEN d.peg_type = 'peggedARS' THEN VALUE :minted :peggedARS :: INTEGER
        END AS minted,
        CASE
            WHEN d.peg_type = 'peggedUSD' THEN VALUE :unreleased :peggedUSD :: INTEGER
            WHEN d.peg_type = 'peggedVAR' THEN VALUE :unreleased :peggedVAR :: INTEGER
            WHEN d.peg_type = 'peggedJPY' THEN VALUE :unreleased :peggedJPY :: INTEGER
            WHEN d.peg_type = 'peggedEUR' THEN VALUE :unreleased :peggedEUR :: INTEGER
            WHEN d.peg_type = 'peggedAUD' THEN VALUE :unreleased :peggedAUD :: INTEGER
            WHEN d.peg_type = 'peggedCAD' THEN VALUE :unreleased :peggedCAD :: INTEGER
            WHEN d.peg_type = 'peggedGBP' THEN VALUE :unreleased :peggedGBP :: INTEGER
            WHEN d.peg_type = 'peggedCNY' THEN VALUE :unreleased :peggedCNY :: INTEGER
            WHEN d.peg_type = 'peggedUAH' THEN VALUE :unreleased :peggedUAH :: INTEGER
            WHEN d.peg_type = 'peggedCHF' THEN VALUE :unreleased :peggedCHF :: INTEGER
            WHEN d.peg_type = 'peggedARS' THEN VALUE :unreleased :peggedARS :: INTEGER
        END AS unreleased,
        f.timestamp,
        VALUE,
        f._inserted_timestamp
    FROM
        {{ ref('bronze__defillama_stablecoin_supply') }}
        f
        LEFT JOIN {{ ref('bronze__defillama_stablecoins') }}
        d
        ON f.stablecoin_id = d.stablecoin_id
),
FINAL AS (
    SELECT
        address,
        symbol,
        NAME AS stablecoin,
        stablecoin_id,
        chains,
        peg_type,
        TIMESTAMP,
        VALUE,
        circulating,
        minted,
        unreleased,
        _inserted_timestamp
    FROM
        expand_flatten
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['stablecoin','timestamp']
    ) }} AS defillama_stablecoin_supply_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
