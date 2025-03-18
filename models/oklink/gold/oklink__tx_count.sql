{{ config(
    materialized = "incremental",
    unique_key = ["tx_count_id"],
    tags = ['oklink']
) }}

WITH source_data AS (
    SELECT
        date_day::DATE AS as_of_date,
        blockchain,
        data,
        _inserted_timestamp
    FROM
        
{% if is_incremental() %}
    {{ ref("bronze_oklink") }}
{% else %}
    {{ ref("bronze_oklink_FR") }}
{% endif %}

    WHERE
        metric = 'stats'
        AND data:code = '0'
        
{% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT 
            MAX(_inserted_timestamp) 
        FROM 
            {{ this }}
    )
{% endif %}
),
flattened_data AS (
    SELECT
        as_of_date,
        blockchain,
        TRY_TO_NUMBER(stats.value:totalTransactionCount) AS tx_count
    FROM
        source_data,
        LATERAL FLATTEN(input => data:data[0]:statsHistoryList) as stats
    WHERE
        stats.value:totalTransactionCount IS NOT NULL
        AND TRY_TO_NUMBER(stats.value:totalTransactionCount) > 0
)

SELECT
    blockchain,
    'tx_count' AS metric,
    tx_count,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['blockchain', 'metric', 'as_of_date']) }} AS tx_count_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM flattened_data
QUALIFY ROW_NUMBER() 
OVER (
    PARTITION BY 
        tx_count_id 
    ORDER BY 
        _inserted_timestamp DESC) = 1