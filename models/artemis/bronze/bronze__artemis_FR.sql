{{ config (
    materialized = 'view'
) }}

WITH raw_data AS (

    {{ streamline_external_table_FR_query_v2(
        model = 'artemis',
        partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER)",
        partition_name = "partition_key",
        other_cols = "to_date(value:DATE_DAY::STRING,'YYYYMMDD') AS DATE_DAY, value:data AS raw_data"
    ) }}
), 
exploded_data AS (
    SELECT
        date_day,
        partition_key,
        _inserted_timestamp,
        file_name,
        blockchain_flat.KEY AS blockchain,
        metric_flat.KEY AS metric,
        metrics.value:date::STRING AS metric_date,
        metrics.value:val AS metric_value
    FROM
        raw_data,
        LATERAL FLATTEN(INPUT => raw_data:data:artemis_ids) AS blockchain_flat,
        LATERAL FLATTEN(INPUT => blockchain_flat.value) AS metric_flat,
        LATERAL FLATTEN(INPUT => metric_flat.value) AS metrics
    WHERE
        raw_data:data:artemis_ids IS NOT NULL
)
SELECT
    TO_DATE(metric_date) AS date_day,
    blockchain,
    metric,
    metric_value,
    partition_key,
    _inserted_timestamp,
    file_name
FROM
    exploded_data