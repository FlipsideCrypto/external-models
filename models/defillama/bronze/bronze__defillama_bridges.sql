{{ config(
    materialized = 'incremental',
    unique_key = 'bridge_id',
    full_refresh = false,
    tags = ['defillama']
) }}

WITH bridge_base AS (

SELECT
    live.udf_api(
        'GET','https://bridges.llama.fi/bridges?includeChains=true',{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp
),

FINAL AS (
SELECT
    VALUE:id::STRING AS bridge_id,
    VALUE:name::STRING AS bridge,
    VALUE:displayName::STRING AS bridge_name,
    VALUE:chains AS chains,
    CASE 
        WHEN VALUE:destinationChain::STRING ilike 'false' OR VALUE:destinationChain::STRING = '-' THEN NULL 
        ELSE VALUE:destinationChain::STRING 
    END AS destination_chain,
    _inserted_timestamp
FROM bridge_base,
    LATERAL FLATTEN (input=> read:data:bridges)

{% if is_incremental() %}
WHERE bridge_id NOT IN (
    SELECT
        DISTINCT bridge_id
    FROM
        {{ this }}
)
)

SELECT
    bridge_id,
    bridge,
    bridge_name,
    chains,
    destination_chain,
    m.row_num + ROW_NUMBER() OVER (ORDER BY bridge) AS row_num,
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
    bridge_id,
    bridge,
    bridge_name,
    chains,
    destination_chain,
    ROW_NUMBER() OVER (ORDER BY bridge) AS row_num,
    _inserted_timestamp
FROM FINAL
{% endif %}