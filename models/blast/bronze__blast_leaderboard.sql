{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    full_refresh = false,
    tags = ['blast']
) }}

WITH api_call AS (

    SELECT
        live.udf_api(
            'GET',
            'https://waitlist-api.prod.blast.io/v1/leaderboard/top-season-2?sortBy=GOLD',{},{}
        ) AS READ,
        'gold' AS leaderboard_type,
        SYSDATE() AS _inserted_timestamp
    UNION ALL
    SELECT
        live.udf_api(
            'GET',
            'https://waitlist-api.prod.blast.io/v1/leaderboard/top-season-2?sortBy=POINTS',{},{}
        ) AS READ,
        'points' AS leaderboard_type,
        SYSDATE() AS _inserted_timestamp
)
SELECT
    SYSDATE() :: DATE AS DATE,
    leaderboard_type,
    VALUE :rank :: INTEGER AS RANK,
    VALUE :displayName :: STRING AS NAME,
    VALUE :points :: INTEGER AS points_amount,
    VALUE :gold :: FLOAT AS gold_amount,
    VALUE :referrerDisplayName :: STRING AS invited_by,
    {{ dbt_utils.generate_surrogate_key(
        ['date', 'rank', 'leaderboard_type']
    ) }} AS id,
    _inserted_timestamp,
FROM
    api_call,
    LATERAL FLATTEN (
        input => READ :data :users
    ) f

{% if is_incremental() %}
WHERE
    _inserted_timestamp :: DATE > (
        SELECT
            MAX(_inserted_timestamp) :: DATE
        FROM
            {{ this }}
    )
{% endif %}
