{{ config(
    materialized = 'incremental',
    unique_key = 'chain',
    tags = ['defillama']
) }}

WITH chain_base AS (

SELECT
    ethereum.streamline.udf_api(
        'GET','https://api.llama.fi/chains',{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp
)

SELECT
    VALUE:chainId::STRING AS chain_id,
    VALUE:name::STRING AS chain,
    VALUE:tokenSymbol::STRING AS token_symbol,
    ROW_NUMBER() OVER (ORDER BY chain) AS row_num,
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
{% endif %}