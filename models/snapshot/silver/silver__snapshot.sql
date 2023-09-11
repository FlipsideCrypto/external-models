{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'delete+insert',
    tags = ['snapshot']
) }}

WITH proposals AS (

    SELECT
        proposal_id,
        ipfs,
        choices,
        proposal_author,
        proposal_title,
        proposal_text,
        space_id,
        network,
        proposal_start_time,
        proposal_end_time,
        _inserted_timestamp
    FROM
        {{ ref('bronze__snapshot_proposals') }}
),
voting_strategy AS (
    SELECT
        proposal_id,
        delay,
        quorum,
        voting_period,
        LOWER(voting_type) AS voting_type
    FROM
        {{ ref('bronze__snapshot_voting_strategy') }}
),
votes AS (
    SELECT
        id,
        ipfs,
        proposal_id,
        voter,
        voting_power,
        vote_timestamp,
        vote_option,
        _inserted_timestamp
    FROM
        {{ ref('bronze__snapshot_votes') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
),
networks AS (
    SELECT
        LOWER(chain) AS network,
        chain_id :: STRING AS chain_id
    FROM
        {{ ref(
            'bronze__defillama_chains'
        ) }}
)
SELECT
    id,
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
    n.network,
    delay,
    quorum,
    voting_period,
    voting_type,
    proposal_start_time,
    proposal_end_time,
    v._inserted_timestamp
FROM
    votes v
    INNER JOIN proposals p
    ON v.proposal_id = p.proposal_id
    LEFT JOIN networks n
    ON p.network = n.chain_id
    LEFT JOIN voting_strategy vs
    ON p.proposal_id = vs.proposal_id
