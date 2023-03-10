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
    protocol,
    address,
    symbol,
    description,
    chain,
    chains,
    category,
    num_audits,
    audit_note
FROM {{ ref('bronze__defillama_protocols') }}