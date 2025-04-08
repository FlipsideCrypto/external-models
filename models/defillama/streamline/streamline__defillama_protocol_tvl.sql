{# {{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"transactions",
        "sql_limit" :"45000",
        "producer_batch_size" :"1",
        "worker_batch_size" :"1",
        "async_concurrent_requests" :"1",
        "sql_source" :"{{this.identifier}}",
        "exploded_key": tojson(["result.transactions"]) }
    ),
    tags = ['streamline_core_history']
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_block_lookback") }}
),
blocks AS (
    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        block_number <= (
            SELECT
                block_number
            FROM
                last_3_days
        )
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_transactions") }}
    WHERE
        block_number <= (
            SELECT
                block_number
            FROM
                last_3_days
        )
)
SELECT
    block_number,
    'QN' as node_name,
    ROUND(
        block_number,
        -3
    )::INT AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        CONCAT(
            '{Service}',
            '/',
            '{Authentication}'
        ),
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        OBJECT_CONSTRUCT(
            'id',
            block_number::STRING,
            'jsonrpc',
            '2.0',
            'method',
            'eth_getBlockByNumber',
            'params',
            ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), TRUE)),
            'Vault/prod/klaytn/quicknode/mainnet'
        ) AS request
        FROM
            blocks
        ORDER BY
            block_number DESC
        LIMIT
            45000 #}