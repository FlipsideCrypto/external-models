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
    funding_date,
    project_name,
    funding_round,
    amount_raised,
    chains,
    sector,
    category,
    category_group,
    source,
    lead_investors,
    other_investors,
    valuation,
    defillama_id,
    raise_id,
    defillama_raises_id,
    inserted_timestamp,
    modified_timestamp,
    _inserted_timestamp,
    _invocation_id
FROM {{ ref('silver__defillama_raises') }} 