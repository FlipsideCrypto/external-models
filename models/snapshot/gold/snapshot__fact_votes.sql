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
    }
) }}

SELECT 
    vote_timestamp,
    LOWER(voter) AS voter, 
    proposal_id,
    voting_power,
    vote_option,
    voting_power,
    ipfs,
    id AS vote_id
FROM 
    {{ ref('silver__snapshot') }}