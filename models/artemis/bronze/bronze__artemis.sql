{{ config (
    materialized = 'view'
) }}
{{ streamline_external_table_query_v2(
    model = 'artemis',
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER)",
    partition_name = "partition_key",
    other_cols = "to_date(value:DATE_DAY::STRING,'YYYYMMDD') AS DATE_DAY, value:data AS raw_data"
) }}