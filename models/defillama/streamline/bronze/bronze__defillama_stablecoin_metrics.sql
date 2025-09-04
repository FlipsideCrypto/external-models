{{ config (
    materialized = 'view',
    tags = ['defillama_streamline']
) }}
{{ streamline_external_table_query_v2(
    model = 'defillama_stablecoin_metrics',
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)",
    partition_name = "partition_key"
) }}
