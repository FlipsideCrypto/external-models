{{ config(
    materialized = 'incremental',
    unique_key = ['date_day', 'blockchain'],
    tags = ['artemis']
) }}

WITH source_data AS (

    SELECT
        date_day :: DATE AS as_of_date,
        blockchain,
        DATA,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__artemis') }}
{% else %}
    {{ ref('bronze__artemis_FR') }}
{% endif %}
WHERE
    metric = 'tx_count'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM {{ this }}
)
{% endif %}
),
flattened_data AS (
    SELECT
        as_of_date,
        blockchain,
        dau.value:date::DATE AS dau_date,
        dau.value:val::INT AS dau_count,
        _inserted_timestamp
    FROM 
        source_data,
        LATERAL FLATTEN(INPUT => DATA:data:artemis_ids[blockchain]:dau) AS dau
    WHERE 
        dau.value:val IS NOT NULL
)
SELECT
    blockchain,
    'active_users' AS metric,
    'The reported number of active users on the as_of_date' AS description,
    as_of_date AS block_date,
    active_users,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['blockchain', 'metric', 'block_date']) }} AS active_users_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flattened_data qualify ROW_NUMBER() over (
        PARTITION BY tx_count_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
