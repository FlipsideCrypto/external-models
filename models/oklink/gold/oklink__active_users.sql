-- depends_on: {{ ref('bronze__oklink') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['as_of_date', 'blockchain'],
    tags = ['oklink'],
    enabled = false
) }}

WITH source AS (

    SELECT
        date_day :: DATE AS as_of_date,
        blockchain,
        TRY_CAST(
            DATA :data [0] :activeAddresses :: STRING AS INT
        ) AS active_users,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref("bronze__oklink") }}
{% else %}
    {{ ref("bronze__oklink_FR") }}
{% endif %}
WHERE
    metric = 'address'
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
    'active_users' AS metric,
    'The reported number of activeAddresses as of the as_of_date' AS description,
    as_of_date,
    active_users,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['blockchain', 'metric', 'as_of_date']) }} AS active_users_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    source qualify ROW_NUMBER() over (
        PARTITION BY blockchain,
        as_of_date
        ORDER BY
            _inserted_timestamp DESC nulls last
    ) = 1
