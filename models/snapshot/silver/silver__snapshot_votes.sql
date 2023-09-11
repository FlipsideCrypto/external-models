{{ config(
    materialized = 'incremental',
    unique_key = 'vote_id',
    incremental_strategy = 'delete+insert',
    tags = ['snapshot']
) }}

SELECT
    vote_timestamp,
    LOWER(voter) AS voter,
    proposal_id,
    vote_option,
    voting_power,
    ipfs,
    id AS vote_id,
    _inserted_timestamp
FROM
    {{ ref('bronze__snapshot_votes') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
