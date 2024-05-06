{{ config (
    materialized = 'view'
) }}

WITH meta AS (

    SELECT
        last_modified AS _inserted_timestamp,
        file_name,
        (
            NULLIF(
                CONCAT(
                    SPLIT_PART(SPLIT_PART(file_name, '/', 2), '-', 1),
                    SPLIT_PART(SPLIT_PART(file_name, '/', 2), '-', 2)
                ),
                'protect_historical.csv'
            ) :: INTEGER
        ) AS _partition_by_month
    FROM
        TABLE(
            information_schema.external_table_file_registration_history(
                start_time => DATEADD('day', -3, CURRENT_TIMESTAMP()),
                table_name => '{{ source( "flashbots", "protect") }}')
            ) A 
        )
    SELECT
        _inserted_timestamp,
        tx_hash,
        from_address,
        to_address,
        created_at_block_number,
        tx_id,
        hints_selected,
        num_of_builders_shared,
        refund_percent,
        s.value
    FROM
        {{ source(
            "flashbots",
            "protect"
        ) }}
        s
        JOIN meta b
        ON b.file_name = metadata$filename
        AND IFNULL(
            b._partition_by_month,
            0
        ) = IFNULL(
            s._partition_by_month,
            0
        )
    WHERE
        IFNULL(
            b._partition_by_month,
            0
        ) = IFNULL(
            s._partition_by_month,
            0
        )
