{{ config(
    materialized = 'incremental',
    unique_key = 'defillama_stablecoin_supply_id',
    full_refresh = false,
    tags = ['defillama']
) }}

WITH 
expand_flatten AS (
    SELECT
        f.address,
        f.symbol,
        f.name,
        f.stablecoin_id,
        d.chains,
        d.peg_type as peg_type,
        CASE 
            WHEN d.peg_type = 'peggedUSD' THEN value:circulating:peggedUSD::INTEGER 
            WHEN d.peg_type = 'peggedVAR' THEN value:circulating:peggedVAR::INTEGER 
            WHEN d.peg_type = 'peggedJPY' THEN value:circulating:peggedJPY::INTEGER 
            WHEN d.peg_type = 'peggedEUR' THEN value:circulating:peggedEUR::INTEGER 
            WHEN d.peg_type = 'peggedAUD' THEN value:circulating:peggedAUD::INTEGER 
            WHEN d.peg_type = 'peggedCAD' THEN value:circulating:peggedCAD::INTEGER 
            WHEN d.peg_type = 'peggedGBP' THEN value:circulating:peggedGBP::INTEGER 
            WHEN d.peg_type = 'peggedCNY' THEN value:circulating:peggedCNY::INTEGER 
            WHEN d.peg_type = 'peggedUAH' THEN value:circulating:peggedUAH::INTEGER 
            WHEN d.peg_type = 'peggedCHF' THEN value:circulating:peggedCHF::INTEGER 
            WHEN d.peg_type = 'peggedARS' THEN value:circulating:peggedARS::INTEGER 
            END 
        AS circulating,
        CASE 
            WHEN d.peg_type = 'peggedUSD' THEN value:minted:peggedUSD::INTEGER 
            WHEN d.peg_type = 'peggedVAR' THEN value:minted:peggedVAR::INTEGER 
            WHEN d.peg_type = 'peggedJPY' THEN value:minted:peggedJPY::INTEGER 
            WHEN d.peg_type = 'peggedEUR' THEN value:minted:peggedEUR::INTEGER 
            WHEN d.peg_type = 'peggedAUD' THEN value:minted:peggedAUD::INTEGER 
            WHEN d.peg_type = 'peggedCAD' THEN value:minted:peggedCAD::INTEGER 
            WHEN d.peg_type = 'peggedGBP' THEN value:minted:peggedGBP::INTEGER 
            WHEN d.peg_type = 'peggedCNY' THEN value:minted:peggedCNY::INTEGER 
            WHEN d.peg_type = 'peggedUAH' THEN value:minted:peggedUAH::INTEGER 
            WHEN d.peg_type = 'peggedCHF' THEN value:minted:peggedCHF::INTEGER 
            WHEN d.peg_type = 'peggedARS' THEN value:minted:peggedARS::INTEGER 
            END 
        AS minted,
        CASE 
            WHEN d.peg_type = 'peggedUSD' THEN value:unreleased:peggedUSD::INTEGER 
            WHEN d.peg_type = 'peggedVAR' THEN value:unreleased:peggedVAR::INTEGER 
            WHEN d.peg_type = 'peggedJPY' THEN value:unreleased:peggedJPY::INTEGER 
            WHEN d.peg_type = 'peggedEUR' THEN value:unreleased:peggedEUR::INTEGER 
            WHEN d.peg_type = 'peggedAUD' THEN value:unreleased:peggedAUD::INTEGER 
            WHEN d.peg_type = 'peggedCAD' THEN value:unreleased:peggedCAD::INTEGER 
            WHEN d.peg_type = 'peggedGBP' THEN value:unreleased:peggedGBP::INTEGER 
            WHEN d.peg_type = 'peggedCNY' THEN value:unreleased:peggedCNY::INTEGER 
            WHEN d.peg_type = 'peggedUAH' THEN value:unreleased:peggedUAH::INTEGER 
            WHEN d.peg_type = 'peggedCHF' THEN value:unreleased:peggedCHF::INTEGER 
            WHEN d.peg_type = 'peggedARS' THEN value:unreleased:peggedARS::INTEGER 
            END 
        AS unreleased,
        f.timestamp,
        value,
        f._inserted_timestamp
    FROM
        {{ ref('bronze__defillama_stablecoin_supply') }} f
    LEFT JOIN
        {{ ref('bronze__defillama_stablecoins') }} d
    ON
        f.stablecoin_id = d.stablecoin_id
{% if is_incremental() %}
WHERE f._inserted_timestamp::DATE > (
        SELECT
            MAX(_inserted_timestamp) :: DATE
        FROM
            {{ this }}
    )
{% endif %}
),
FINAL AS (
    select
        address,
        symbol,
        name as stablecoin,
        stablecoin_id,
        chains,
        peg_type,
        timestamp,
        value,
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
