-- depends_on: {{ ref('bronze__bitquery') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['blockchain', 'metric', 'as_of_date'],
    tags = ['bitquery']
) }}

WITH base AS(

    SELECT
        A.blockchain,
        A.metric,
        A.date_Day AS as_of_date,
        COALESCE(
            REGEXP_SUBSTR(
                A.data,
                '"countBigInt"\\s*:\\s*"([^"]+)"',
                1,
                1,
                'e',
                1
            ),
            REGEXP_SUBSTR(
                A.data,
                '"senders"\\s*:\\s*"([^"]+)"',
                1,
                1,
                'e',
                1
            )
        ) active_users,
        A._inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__bitquery') }}
{% else %}
    {{ ref('bronze__bitquery_FR') }}
{% endif %}

A
WHERE
    A.data :errors IS NULL
    AND A.metric = 'active_users'
    AND active_users IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    blockchain,
    metric,
    as_of_date,
    active_users,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['blockchain','metric','as_of_date']
    ) }} AS accounts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base qualify ROW_NUMBER() over (
        PARTITION BY blockchain,
        metric,
        as_of_date
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
