-- depends_on: {{ ref('bronze__bitquery') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['blockchain', 'metric', 'as_of_date'],
    tags = ['bitquery']
) }}

WITH ripple AS (

    SELECT
        A.blockchain,
        A.metric,
        A.date_Day AS as_of_date,
        b.value :countBigInt AS active_users,
        A._inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__bitquery') }}
{% else %}
    {{ ref('bronze__bitquery_FR') }}
{% endif %}

A,
LATERAL FLATTEN(
    A.data :data :ripple :transactions
) b
WHERE
    A.data :errors IS NULL
    AND A.metric = 'active_users'
    AND A.blockchain = 'ripple'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE > (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
hedera AS (
    SELECT
        A.blockchain,
        A.metric,
        A.date_Day AS as_of_date,
        b.value :countBigInt AS active_users,
        A._inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__bitquery_FR') }}
{% else %}
    {{ ref('bronze__bitquery_FR') }}
{% endif %}

A,
LATERAL FLATTEN(
    A.data :data :hedera :transactions
) b
WHERE
    A.data :errors IS NULL
    AND A.metric = 'active_users'
    AND A.blockchain = 'hedera'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE > (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
ua AS (
    SELECT
        *
    FROM
        ripple
    UNION ALL
    SELECT
        *
    FROM
        hedera
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
    ua qualify ROW_NUMBER() over (
        PARTITION BY blockchain,
        metric,
        as_of_date
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
