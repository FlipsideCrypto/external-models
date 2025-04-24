-- depends_on: {{ ref("bronze__artemis")}}
-- depends_on: {{ ref("bronze__artemis_FR")}}
{{ config(
    materialized = "incremental",
    unique_key = ['metric_date', 'blockchain', 'metric'],
    tags = ['silver', 'artemis']
) }}

WITH source_data AS (

    SELECT
        raw_data,
        partition_key,
        _inserted_timestamp,
        file_name
    FROM

{% if is_incremental() %}
{{ ref('bronze__artemis') }}
{% else %}
    {{ ref('bronze__artemis_FR') }}
{% endif %}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
parsed_data AS (
    SELECT
        s._inserted_timestamp,
        s.partition_key,
        s.file_name,
        blockchain_flat.key AS blockchain,
        metric_flat.key AS metric,
        metrics.value :date :: STRING AS metric_date,
        metrics.value :val AS metric_value
    FROM
        source_data s,
        LATERAL FLATTEN(
            input => raw_data :data :artemis_ids
        ) AS blockchain_flat,
        LATERAL FLATTEN(
            input => blockchain_flat.value
        ) AS metric_flat,
        LATERAL FLATTEN(
            input => metric_flat.value
        ) AS metrics
    WHERE
        raw_data :data :artemis_ids IS NOT NULL
)
SELECT
    TO_DATE(metric_date) AS metric_date,
    blockchain,
    metric,
    metric_value,
    partition_key,
    _inserted_timestamp,
    file_name,
    {{ dbt_utils.generate_surrogate_key(['blockchain', 'metric', 'metric_date']) }} AS artemis_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    parsed_data qualify(ROW_NUMBER() over (PARTITION BY metric_date, blockchain, metric
ORDER BY
    _inserted_timestamp DESC) = 1)
