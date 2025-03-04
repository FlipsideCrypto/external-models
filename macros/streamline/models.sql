{% macro streamline_external_table_query_v2(
        model,
        partition_function,
        partition_name,
        other_cols
) %}
WITH meta AS (
    SELECT
        LAST_MODIFIED::timestamp_ntz AS _inserted_timestamp,
        file_name,
        {{ partition_function }} AS {{ partition_name }}
    FROM
        TABLE(
            information_schema.external_table_file_registration_history(
                start_time => DATEADD('day', -3, CURRENT_TIMESTAMP()),
                table_name => '{{ source( "bronze_streamline", model) }}')
            ) A
)
SELECT
    {{ other_cols }},
    DATA,
    _inserted_timestamp,
    s.{{ partition_name }},
    s.value AS VALUE,
    file_name
FROM
    {{ source(
        "bronze_streamline",
        model
    ) }}
    s
JOIN 
    meta b
    ON b.file_name = metadata$filename
    AND b.{{ partition_name }} = s.{{ partition_name }}
WHERE
    b.{{ partition_name }} = s.{{ partition_name }}
    AND (
        data:error:code IS NULL
    )
{% endmacro %}

{% macro streamline_external_table_FR_query_v2(
        model,
        partition_function,
        partition_name,
        other_cols
) %}
WITH meta AS (
    SELECT
        LAST_MODIFIED::timestamp_ntz AS _inserted_timestamp,
        file_name,
        {{ partition_function }} AS {{ partition_name }}
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", model) }}'
            )
        ) A
)
SELECT
    {{ other_cols }},
    DATA,
    _inserted_timestamp,
    s.{{ partition_name }},
    s.value AS VALUE,
    file_name
FROM
    {{ source(
        "bronze_streamline",
        model
    ) }}
    s
JOIN 
    meta b
    ON b.file_name = metadata$filename
    AND b.{{ partition_name }} = s.{{ partition_name }}
WHERE
    b.{{ partition_name }} = s.{{ partition_name }}
    AND (
        data:error:code IS NULL
    )
{% endmacro %}