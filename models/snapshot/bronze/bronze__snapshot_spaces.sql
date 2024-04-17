{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'space_id',
    full_refresh = false,
    tags = ['snapshot']
) }}

WITH requests AS ({% for item in range(6) %}
    (

    SELECT
        live.udf_api('GET', 'https://hub.snapshot.org/graphql',{ 'apiKey':'key' },{ 'query': 'query { spaces(orderBy: "created", orderDirection: desc, first: 1000, skip: ' || {{ item * 1000 }} || ') { id name about network symbol admins members categories domain private treasuries { address name network } verified } }' },'Vault/prod/external/snapshot') AS resp, SYSDATE() AS _inserted_timestamp) {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}),
    FINAL AS (
        SELECT
            VALUE :id :: STRING AS space_id,
            VALUE :name :: STRING AS SPACE,
            VALUE :about :: STRING AS about,
            VALUE :network :: STRING AS network,
            VALUE :symbol :: STRING AS symbol,
            VALUE :admins :: variant AS admins,
            VALUE :members :: variant AS members,
            VALUE :categories :: variant AS categories,
            VALUE :domain :: STRING AS domain,
            VALUE :private :: BOOLEAN AS is_private,
            VALUE :treasuries :: variant AS treasuries,
            VALUE :verified :: BOOLEAN AS is_verified,
            _inserted_timestamp
        FROM
            requests,
            LATERAL FLATTEN(
                input => resp :data :data :spaces
            ) qualify(ROW_NUMBER() over(PARTITION BY space_id
        ORDER BY
            _inserted_timestamp DESC)) = 1
    )
SELECT
    space_id,
    SPACE,
    about,
    symbol,
    network,
    categories,
    domain,
    is_private,
    is_verified,
    admins,
    members,
    treasuries,
    _inserted_timestamp
FROM
    FINAL
