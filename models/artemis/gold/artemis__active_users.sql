{{ config(
    materialized = 'incremental',
    unique_key = ['block_date', 'blockchain', 'metric'],
    tags = ['artemis']
) }}

SELECT
    blockchain,
    'active_users' AS metric,
    'The reported number of active users as of the block_date' AS description,
    metric_date AS block_date,
    metric_value :: INT AS active_users,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['blockchain', 'metric', 'block_date']) }} AS active_users_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__artemis') }}
WHERE
    metric = 'dau'
    AND NULLIF(TRIM(metric_value), 'null') IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        COALESCE(MAX(_inserted_timestamp), '1970-01-01' :: timestamp_ntz)
    FROM
        {{ this }})
    {% endif %}

    qualify ROW_NUMBER() over (
        PARTITION BY active_users_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
