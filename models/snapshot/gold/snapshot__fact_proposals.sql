{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'SNAPSHOT',
                'PURPOSE': 'GOVERNANCE'
            }
        }
    },
    tags = ['snapshot']
) }}

SELECT 
    proposal_title,
    proposal_author,
    proposal_text,
    choices,
    delay,
    quorum,
    voting_period,
    voting_type,
    proposal_start_time,
    proposal_end_time,
    created_at,
    space_id,
    network,
    ipfs,
    proposal_id
FROM 
    {{ ref('silver__snapshot') }}