{{ config (
    materialized = 'view',
    tags = ['deepnftvalue']
) }}

WITH meta AS (

    SELECT
        registered_on AS _inserted_timestamp,
        file_name,
        CONCAT(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1), '-01') :: DATE AS DATE_PART
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "valuations_parquet") }}'
            )
        ) A
)
SELECT
    s.date_part,
    _inserted_timestamp,
    collection_address,
    collection_name,
    token_id,
    price,
    valuation_date,
    currency,
    collection_slug,
    metadata$filename AS _filename
FROM
    {{ source(
        "bronze_streamline",
        "valuations_parquet"
    ) }}
    s
    JOIN meta b
    ON b.file_name = metadata$filename
    AND b.date_part = s.date_part
WHERE
    b.date_part = s.date_part
