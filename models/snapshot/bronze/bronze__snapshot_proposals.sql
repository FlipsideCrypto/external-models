{{ config(
    materialized = 'incremental',
    unique_key = 'proposal_id',
    full_refresh = false,
    tags = ['snapshot']
) }}

WITH requests AS ({% for item in range(6) %}
    (

    SELECT
        live.udf_api('GET', 'https://hub.snapshot.org/graphql',{ 'apiKey':'key' },
        { 'query':'query { proposals(orderBy: "created", orderDirection: asc,first:1000, skip: ' || {{ item * 1000 }} || ',where:{created_gte: ' || max_time_start || '}) { id space{id voting {delay quorum period type}} ipfs author created network type title body start end state votes choices scores_state scores } }' }
        ,'Vault/prod/external/snapshot') AS resp, 
        SYSDATE() AS _inserted_timestamp
    FROM
        (
    SELECT
        DATE_PART(epoch_second, max_prop_created :: TIMESTAMP) AS max_time_start
    FROM
        (

{% if is_incremental() %}
SELECT
    MAX(created_at) AS max_prop_created
FROM
    {{ this }}
{% else %}
SELECT
    1595080000 AS max_prop_created
{% endif %}) AS max_time)) {% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}),
FINAL AS (
    SELECT
        VALUE :id :: STRING AS proposal_id,
        VALUE :ipfs :: STRING AS ipfs,
        STRTOK_TO_ARRAY(
            VALUE :choices,
            ';'
        ) AS choices,
        VALUE :author :: STRING AS proposal_author,
        VALUE :title :: STRING AS proposal_title,
        VALUE :body :: STRING AS proposal_text,
        VALUE :space :id :: STRING AS space_id,
        VALUE :space :voting :delay :: INTEGER AS delay,
        VALUE :space :voting :quorum :: INTEGER AS quorum,
        VALUE :space :voting :period :: INTEGER AS voting_period,
        VALUE :space :voting :type :: STRING AS voting_type,
        VALUE :network :: STRING AS network,
        TO_TIMESTAMP_NTZ(
            VALUE :created
        ) AS created_at,
        TO_TIMESTAMP_NTZ(
            VALUE :start
        ) AS proposal_start_time,
        TO_TIMESTAMP_NTZ(
            VALUE :end
        ) AS proposal_end_time,
        VALUE,
        _inserted_timestamp
    FROM
        requests,
        LATERAL FLATTEN(
            input => resp :data :data :proposals
        )
    qualify(ROW_NUMBER() over (PARTITION BY proposal_id
    ORDER BY
        TO_TIMESTAMP_NTZ(VALUE :created) DESC)) = 1
)
SELECT
    proposal_id,
    ipfs,
    choices,
    proposal_author,
    proposal_title,
    proposal_text,
    (delay / 3600) :: INTEGER AS delay,
    quorum,
    (voting_period / 3600) :: INTEGER AS voting_period,
    LOWER(voting_type) AS voting_type,
    space_id,
    network,
    created_at,
    proposal_start_time,
    proposal_end_time,
    _inserted_timestamp
FROM
    FINAL
