{{ config(
    materialized = 'incremental',
    unique_key = 'raise_id',
    tags = ['defillama']
) }}

WITH protocol_base AS (

SELECT
    live.udf_api(
        'GET','https://pro-api.llama.fi/{api_key}/api/raises',{},{},'Vault/prod/external/defillama'
    ) AS read,
    SYSDATE() AS _inserted_timestamp
)

SELECT
    TRY_TO_TIMESTAMP(VALUE:date::STRING) AS funding_date,
    VALUE:name::STRING AS project_name,
    VALUE:round::STRING AS funding_round,
    VALUE:amount::FLOAT AS amount_raised,
    VALUE:chains::VARIANT AS chains,
    VALUE:sector::STRING AS sector,
    VALUE:category::STRING AS category,
    VALUE:categoryGroup::STRING AS category_group,
    VALUE:source::STRING AS source,
    VALUE:leadInvestors::VARIANT AS lead_investors,
    VALUE:otherInvestors::VARIANT AS other_investors,
    TRY_TO_NUMBER(VALUE:valuation::STRING) AS valuation,
    VALUE:defillamaId::STRING AS defillama_id,
    MD5(CONCAT(
        COALESCE(VALUE:name::STRING, ''),
        COALESCE(VALUE:date::STRING, ''),
        COALESCE(VALUE:round::STRING, ''),
        COALESCE(VALUE:amount::STRING, '')
    )) AS raise_id,
    _inserted_timestamp
FROM protocol_base,
    LATERAL FLATTEN (input=> read :data :raises)

{% if is_incremental() %}
WHERE raise_id NOT IN (
    SELECT
        DISTINCT raise_id
    FROM
        {{ this }}
)
{% endif %}

QUALIFY(
    ROW_NUMBER() OVER (PARTITION BY raise_id ORDER BY _inserted_timestamp DESC)
) = 1