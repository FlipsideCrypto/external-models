{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'SNAPSHOT',
    'PURPOSE': 'GOVERNANCE' } } },
    tags = ['snapshot']
) }}

SELECT
    created_at,
    proposal_title,
    proposal_author,
    proposal_text,
    choices,
    delay,
    quorum,
    voting_period,
    voting_type,
    ipfs,
    proposal_start_time,
    proposal_end_time,
    space_id,
    network,
    network_id,
    proposal_id
FROM
    {{ ref('silver__snapshot_proposals') }}