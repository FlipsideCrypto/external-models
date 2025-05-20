{{ config(
    materialized = 'incremental',
    unique_key = 'defillama_bridge_vol_by_chain_id',
    tags = ['defillama']
) }}

WITH list_of_bridges AS (

    SELECT
        bridge_id,
        bridge,
        bridge_name,
        LOWER(
            VALUE :: STRING
        ) AS chain
    FROM
        {{ ref('bronze__defillama_bridges') }},
        LATERAL FLATTEN (
            input => chains
        )
    WHERE
        chain IN (
            SELECT
                DISTINCT blockchain
            FROM
                {{ source(
                    'crosschain_defi',
                    'ez_bridge_activity'
                ) }}
        )
),

build_requests as (
    select 
    bridge_id, 
    bridge, 
    bridge_name, 
    chain, 
    CONCAT(bridge_id, '-', chain) AS bridge_id_chain, 
    ROW_NUMBER() over (
            ORDER BY
                bridge_id_chain
        ) AS row_number_request
FROM
    list_of_bridges
    {% if is_incremental() %}
where
    bridge_id_chain NOT IN 
    (
SELECT
    bridge_id_chain
FROM
    (
SELECT
    bridge_id_chain,
    MAX(TIMESTAMP :: DATE) AS max_timestamp
FROM
    {{ this }}
GROUP BY 
    ALL 
HAVING
    SYSDATE()::date = max_timestamp)
    )
{% endif %}) ,

requests AS (
    {% for item in range(10) %}
SELECT
    bridge_id, 
    bridge, 
    bridge_name, 
    chain, 
    bridge_id_chain, 
    live.udf_api('GET', CONCAT('https://bridges.llama.fi/bridgevolume/', chain, '?id=', bridge_id),{},{}) AS READ
FROM
    build_requests
WHERE
    row_number_request BETWEEN {{ item * 40 + 1 }}
    AND {{(item + 1) * 40 }}

{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %})
SELECT
    bridge_id,
    bridge,
    bridge_name,
    chain,
    bridge_id_chain,
    TO_TIMESTAMP(
        VALUE :date :: INTEGER
    )::DATE AS date,
    VALUE :depositTxs :: INTEGER AS deposit_txs,
    VALUE :depositUSD :: INTEGER AS deposit_usd,
    VALUE :withdrawTxs :: INTEGER AS withdraw_txs,
    VALUE :withdrawUSD :: INTEGER AS withdraw_usd,
    SYSDATE() as inserted_timestamp,
    SYSDATE() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['bridge_id_chain', 'date']) }} AS defillama_bridge_vol_by_chain_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    requests,
    LATERAL FLATTEN (
        input => READ :data
    )
