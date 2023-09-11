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

WITH votes AS (
    SELECT
        vote_id,
        ipfs,
        proposal_id,
        voter,
        voting_power,
        vote_timestamp,
        vote_option,
        _inserted_timestamp
    FROM
        {{ ref('silver__snapshot_votes') }}
),
proposals AS (

    SELECT
        proposal_id,
        ipfs,
        choices,
        proposal_author,
        proposal_title,
        proposal_text,
        delay,
        quorum,
        voting_period,
        voting_type,
        space_id,
        network,
        network_id,
        proposal_start_time,
        proposal_end_time,
        _inserted_timestamp
    FROM
        {{ ref('silver__snapshot_proposals') }}
)
SELECT
    vote_id AS id, 
    v.proposal_id, 
    voter, 
    vote_option, 
    voting_power, 
    vote_timestamp, 
    choices, 
    proposal_author, 
    proposal_title, 
    proposal_text, 
    space_id,
    network, 
    delay, 
    quorum, 
    voting_period, 
    voting_type, 
    proposal_start_time, 
    proposal_end_time
FROM
    votes v
    INNER JOIN proposals p
    ON v.proposal_id = p.proposal_id