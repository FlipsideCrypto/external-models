{{ config(
    materialized = 'incremental',
    unique_key = 'protocol_id',
    tags = ['defillama']
) }}

WITH protocol_base AS (

SELECT
    live.udf_api(
        'GET','https://api.llama.fi/protocols',{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp
)

SELECT
    VALUE:id::STRING AS protocol_id,
    VALUE:slug::STRING AS protocol_slug,
    VALUE:name::STRING AS protocol,
    CASE 
        WHEN VALUE:address::STRING = '-' THEN NULL 
        ELSE SUBSTRING(LOWER(VALUE:address::STRING), CHARINDEX(':', LOWER(VALUE:address::STRING))+1) 
    END AS address,
    CASE 
        WHEN VALUE:symbol::STRING = '-' THEN NULL 
        ELSE VALUE:symbol::STRING 
    END AS symbol,
    VALUE:description::STRING AS description,
    VALUE:chain::STRING AS chain,
    VALUE:audits::INTEGER AS num_audits,
    VALUE:audit_note::STRING AS audit_note,
    VALUE:category::STRING AS category,
    VALUE:chains AS chains,
    ROW_NUMBER() over (
        ORDER BY
        protocol_id 
    ) AS row_num,
    _inserted_timestamp
FROM protocol_base,
    LATERAL FLATTEN (input=> read:data)

{% if is_incremental() %}
WHERE protocol_id NOT IN (
    SELECT
        DISTINCT protocol_id
    FROM
        {{ this }}
)
{% endif %}