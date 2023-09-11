{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'address',
    full_refresh = false,
    tags = ['snapshot']
) }}

WITH requests AS ({% for item in range(6) %}
    (

    SELECT
        ethereum.streamline.udf_api('GET', 'https://hub.snapshot.org/graphql',{ 'apiKey': (
    SELECT
        api_key
    FROM
        {{ source('crosschain_silver', 'apis_keys') }}
    WHERE
        api_name = 'snapshot') },{ 'query': 'query { users(orderBy: "created", orderDirection: asc, first: 1000, skip: ' || {{ item * 1000 }} || ', where:{created_gte: ' || max_time_start || ',created_lt: ' || max_time_end || '}) { id name about avatar ipfs created } }' }) AS resp, SYSDATE() AS _inserted_timestamp
    FROM
        (
    SELECT
        DATE_PART(epoch_second, max_start :: TIMESTAMP) AS max_time_start, 
        DATE_PART(epoch_second, max_start :: TIMESTAMP) + 86400 AS max_time_end
    FROM
        (

{% if is_incremental() %}
SELECT
    MAX(created_at) AS max_start
FROM
    {{ this }}
{% else %}
SELECT
    1595000000 AS max_start
{% endif %}) AS max_time)) {% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}),
FINAL AS (
    SELECT
        LOWER(
            VALUE :id :: STRING
        ) AS address,
        VALUE :name :: STRING AS NAME,
        VALUE :about :: STRING AS about,
        VALUE :avatar :: STRING AS avatar,
        VALUE :ipfs :: STRING AS ipfs,
        TO_TIMESTAMP_NTZ(
            VALUE :created
        ) AS created_at,
        _inserted_timestamp
    FROM
        requests,
        LATERAL FLATTEN(
            input => resp :data :data :users
        ) qualify(ROW_NUMBER() over(PARTITION BY address
    ORDER BY
        TO_TIMESTAMP_NTZ(VALUE :created) DESC)) = 1
)
SELECT
    address,
    NAME,
    about,
    avatar,
    ipfs,
    created_at,
    _inserted_timestamp
FROM
    FINAL
