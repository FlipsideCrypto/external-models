{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"defillama_protocol_historical",
        "sql_limit" :"10",
        "producer_batch_size" :"1",
        "worker_batch_size" :"1",
        "async_concurrent_requests" :"1",
        "sql_source" :"{{this.identifier}}" }
    ),
    tags = ['streamline_defillama']
) }}

WITH base AS (

    SELECT
        protocol_slug,
        protocol_id,
        row_num
    FROM
        {{ ref('bronze__defillama_protocols') }}
    WHERE
        row_num = 10
)
SELECT
    round(protocol_id,-1) AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        'https://pro-api.llama.fi/{api_key}/api/hourly/' || protocol_slug
        ,{}
        ,{}
        ,'Vault/prod/external/defillama'
    ) AS request
FROM
    base
order by 
    row_num ASC