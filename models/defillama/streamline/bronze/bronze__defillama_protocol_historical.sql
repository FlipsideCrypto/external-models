{{ config (
    materialized = 'view'
) }}
{{ streamline_external_table_query_v2(
    model = 'defillama_protocol_historical',
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER)",
    partition_name = "partition_key",
    other_cols = "value:PROTOCOL_ID::INTEGER AS protocol_id"
) }}
