{{ config (
    materialized = 'view'
) }}

SELECT
    id, 
    proposal_id, 
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
    proposal_end_time,
    _inserted_timestamp
FROM
    {{ source(
        'ethereum_silver',
        'snapshot'
    ) }}