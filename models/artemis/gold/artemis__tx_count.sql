{{ config(
    materialized = 'incremental',
    unique_key = ['date_day', 'blockchain'],
    tags = ['artemis']
) }}

WITH source_data AS (

    SELECT
        date_day :: DATE AS as_of_date,
        blockchain,
        DATA,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__artemis') }}
{% else %}
    {{ ref('bronze__artemis_FR') }}
{% endif %}
WHERE
    metric = 'tx_count'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM {{ this }}
)
{% endif %}
),
flattened_data AS (
    SELECT
        as_of_date,
        blockchain,
        tx.value:date::DATE AS tx_date,
        tx.value:val::INT AS tx_count,
        _inserted_timestamp
    FROM 
        source_data,
        LATERAL FLATTEN(INPUT => DATA:data:artemis_ids[blockchain]:daily_txns) AS tx
    WHERE 
        tx.value:val IS NOT NULL
)
SELECT
    blockchain,
    'tx_count' AS metric,
    'The reported number of transactions on the as_of_date' AS description,
    as_of_date AS block_date,
    tx_count,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['blockchain', 'metric', 'block_date']) }} AS tx_count_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flattened_data qualify ROW_NUMBER() over (
        PARTITION BY tx_count_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
