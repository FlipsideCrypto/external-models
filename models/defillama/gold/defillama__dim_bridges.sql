{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['defillama']
) }}

SELECT
    bridge_id,
    bridge,
    chains,
    destination_chain
FROM {{ ref('bronze__defillama_bridges') }}