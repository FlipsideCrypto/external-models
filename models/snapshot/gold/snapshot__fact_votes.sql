{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'SNAPSHOT',
    'PURPOSE': 'GOVERNANCE' } } },
    tags = ['snapshot']
) }}

SELECT
    vote_timestamp,
    voter,
    proposal_id,
    vote_option,
    voting_power,
    ipfs,
    vote_id
FROM
    {{ ref('silver__snapshot_votes') }}
