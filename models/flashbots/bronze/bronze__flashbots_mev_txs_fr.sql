{{ config (
    materialized = 'view'
) }}

WITH meta AS (

    SELECT
        registered_on AS _inserted_timestamp,
        file_name,
        (
            NULLIF(
                CONCAT(
                    SPLIT_PART(SPLIT_PART(file_name, '/', 3), '-', 1),
                    SPLIT_PART(SPLIT_PART(file_name, '/', 3), '-', 2)
                ),
                'mevshare_historical.csv'
            ) :: INTEGER
        ) AS _partition_by_month
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "flashbots", "mev") }}'
            )) A 
        )
    SELECT
        _inserted_timestamp,
        block_number,
        block_time,
        block_hash,
        extra_data,
        fee_recipient_address,
        bundle_id,
        user_tx_hash,
        user_tx_from,
        user_tx_to,
        backrun_tx_hash,
        backrun_tx_from,
        backrun_tx_to,
        refund_tx_hash,
        refund_from,
        refund_to,
        refund_value_eth,
        is_mevshare,
        s.value
    FROM
        {{ source(
            "flashbots",
            "mev"
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
