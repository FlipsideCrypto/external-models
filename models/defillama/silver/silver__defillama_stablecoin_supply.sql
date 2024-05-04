{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    tags = ['defillama']
) }}

WITH stablecoin_base AS (

{% for item in range(4) %}
(
SELECT
    stablecoin_id,
    stablecoin,
    symbol,
    live.udf_api(
        'GET',concat('https://stablecoins.llama.fi/stablecoin/',stablecoin_id),{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp,
FROM (
    SELECT 
        stablecoin_id,
        stablecoin,
        symbol,
        row_num
    FROM external_dev.bronze.defillama_stablecoins
    WHERE row_num BETWEEN {{ item * 20 + 1 }} AND {{ (item + 1) * 20 }} and stablecoin_id not in (1,2))
    {% if is_incremental() %}
    WHERE stablecoin_id NOT IN (
    SELECT
        stablecoin_id
    FROM (
        SELECT 
            DISTINCT stablecoin_id,
            MAX(timestamp::DATE) AS max_timestamp
        FROM {{ this }}
        GROUP BY 1
        HAVING CURRENT_DATE = max_timestamp
    ))
{% endif %}
){% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
UNION ALL
(
SELECT
    stablecoin_id,
    stablecoin,
    symbol,
    live.udf_api(
        'GET','https://stablecoins.llama.fi/stablecoin/1',{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp,
FROM (
    SELECT 
        stablecoin_id,
        stablecoin,
        symbol,
        row_num
    FROM external_dev.bronze.defillama_stablecoins
    WHERE stablecoin_id =1)
    {% if is_incremental() %}
    WHERE stablecoin_id NOT IN (
    SELECT
        stablecoin_id
    FROM (
        SELECT 
            DISTINCT stablecoin_id,
            MAX(timestamp::DATE) AS max_timestamp
        FROM {{ this }}
        GROUP BY 1
        HAVING CURRENT_DATE = max_timestamp
    ))
    {% endif %}
)
union all
(
SELECT
    stablecoin_id,
    stablecoin,
    symbol,
    live.udf_api(
        'GET','https://stablecoins.llama.fi/stablecoin/2',{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp,
FROM (
    SELECT 
        stablecoin_id,
        stablecoin,
        symbol,
        row_num
    FROM external_dev.bronze.defillama_stablecoins
    WHERE stablecoin_id =2)
    {% if is_incremental() %}
    WHERE stablecoin_id NOT IN (
    SELECT
        stablecoin_id
    FROM (
        SELECT 
            DISTINCT stablecoin_id,
            MAX(timestamp::DATE) AS max_timestamp
        FROM {{ this }}
        GROUP BY 1
        HAVING CURRENT_DATE = max_timestamp
    ))
    {% endif %}
)
),
flatten AS (
SELECT
    read:data:address::string as address,
    read:data:symbol::string as symbol,
    read:data:name::string as name,
    read:data:id::string as stablecoin_id,
    value AS value,
    TO_TIMESTAMP(VALUE:date::INTEGER) AS timestamp,
    
    _inserted_timestamp
FROM stablecoin_base,
    LATERAL FLATTEN (input=> read:data:tokens) f
),
FINAL AS (
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
        flatten f
    LEFT JOIN
        {{ ref('bronze__defillama_stablecoins') }} d
    ON
        f.stablecoin_id = d.stablecoin_id
)
select
    address,
    symbol,
    name,
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
    FINAL