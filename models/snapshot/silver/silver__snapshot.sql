{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'delete+insert'
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
    {{ ref('bronze__snapshot') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) :: DATE
        FROM
            {{ this }}
    )
{% endif %}
