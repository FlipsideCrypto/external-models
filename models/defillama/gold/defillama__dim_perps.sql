{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['defillama'],
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'DEFILLAMA'
            }
        }
    }
) }}

SELECT
    protocol_id,
    protocol_slug,
    NAME as protocol,
    category,
    chains,
    parent_protocol,
    linked_protocols
FROM
    {{ ref('bronze__defillama_perps') }}