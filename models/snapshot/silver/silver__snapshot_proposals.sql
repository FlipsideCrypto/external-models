{{ config(
    materialized = 'incremental',
    unique_key = 'proposal_id',
    incremental_strategy = 'delete+insert',
    tags = ['snapshot']
) }}

SELECT
    proposal_id,
    ipfs,
    choices,
    LOWER(proposal_author) AS proposal_author,
    proposal_title,
    proposal_text,
    delay,
    quorum,
    voting_period,
    voting_type,
    space_id,
    p.network AS network_id,
    LOWER(chain) AS network,
    created_at,
    proposal_start_time,
    proposal_end_time,
    p._inserted_timestamp
FROM
    {{ ref('bronze__snapshot_proposals') }}
    p
    LEFT JOIN {{ ref('bronze__defillama_chains') }}
    d
    ON d.chain_id :: STRING = p.network :: STRING

{% if is_incremental() %}
WHERE
    p._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY proposal_id
ORDER BY
    p._inserted_timestamp DESC)) = 1
