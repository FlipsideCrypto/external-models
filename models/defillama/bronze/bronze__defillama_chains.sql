{{ config(
    materialized = 'incremental',
    unique_key = 'chain',
    tags = ['defillama']
) }}

WITH chain_base AS (

SELECT
    live.udf_api(
        'GET','https://pro-api.llama.fi/{api_key}/api/chains',{},{},'Vault/prod/external/defillama'
    ) AS read,
    SYSDATE() AS _inserted_timestamp
),

FINAL AS (
SELECT
    VALUE:chainId::STRING AS chain_id,
    VALUE:name::STRING AS chain,
    VALUE:tokenSymbol::STRING AS token_symbol,
    _inserted_timestamp
FROM chain_base,
    LATERAL FLATTEN (input=> read:data)

{% if is_incremental() %}
WHERE chain NOT IN (
    SELECT
        DISTINCT chain
    FROM
        {{ this }}
)
)

SELECT
    chain_id,
    chain,
    token_symbol,
    m.row_num + ROW_NUMBER() OVER (ORDER BY chain) AS row_num,
    _inserted_timestamp
FROM FINAL
JOIN (
    SELECT
        MAX(row_num) AS row_num
    FROM
        {{ this }}
) m ON 1=1

{% else %}
)
SELECT
    chain_id,
    chain,
    token_symbol,
    ROW_NUMBER() OVER (ORDER BY chain) AS row_num,
    _inserted_timestamp
FROM FINAL
{% endif %}