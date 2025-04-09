-- depends_on: {{ ref("bronze__artemis")}}
-- depends_on: {{ ref("bronze__artemis_FR")}}
{{ config (
    materialized = "incremental",
    unique_key = ['date_day'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['streamline_realtime']
) }}

WITH bronze AS (
    
    SELECT
        date_day AS request_date_day,
        data,
        partition_key,
        _inserted_timestamp,
        file_name
    FROM

    {% if is_incremental() %}
    {{ ref('bronze__artemis') }}
    {% else %}
        {{ ref('bronze__artemis_FR') }}
    {% endif %}
    WHERE
        DATA IS NOT NULL

    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
        {% endif %}
),
extracted_dates AS (
    SELECT
        request_date_day,
        TO_DATE(date_vals.value:date::STRING) AS extracted_date,
        data,
        partition_key,
        _inserted_timestamp,
        file_name
    FROM
        bronze,
        LATERAL FLATTEN(INPUT => data:data:artemis_ids) AS blockchain_flat,
        LATERAL FLATTEN(INPUT => blockchain_flat.value) AS metric_flat,
        LATERAL FLATTEN(INPUT => metric_flat.value) AS date_vals
    WHERE
        data:data:artemis_ids IS NOT NULL
)
SELECT
    extracted_date as date_day,
    partition_key,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    file_name,
    '{{ invocation_id }}' AS _invocation_id
FROM
    extracted_dates

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY extracted_date
    ORDER BY
        _inserted_timestamp DESC
) = 1
