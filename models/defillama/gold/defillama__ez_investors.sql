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
    investor,
    deals,
    total_amount,
    median_amount,
    chains,
    top_project_category,
    top_round_type,
    projects,
    defillama_investors_id,
    inserted_timestamp,
    modified_timestamp,
    _inserted_timestamp,
    _invocation_id
FROM {{ ref('silver__defillama_investors') }} 