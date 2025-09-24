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
    category,
    NAME,
    display_name,
    module,
    logo,
    chains,
    protocol_type,
    methodology_url,
    methodology,
    parent_protocol,
    linked_protocols,
    _inserted_timestamp
FROM
    {{ ref('bronze__defillama_perps') }}