{{ config(
    materialized = 'incremental',
    unique_key = ['block_date', 'blockchain', 'metric'],
    tags = ['artemis']
) }}

SELECT
    blockchain,
    'tx_count' AS metric,
    'The reported number of transactions as of the block_date' AS description,
    metric_date AS block_date,
    metric_value AS tx_count,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['blockchain', 'metric', 'block_date']) }} AS tx_count_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    {{ ref('silver__artemis') }}
WHERE
    metric = 'daily_txns'

    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT COALESCE(MAX(_inserted_timestamp), '1970-01-01'::TIMESTAMP_NTZ)
        FROM {{ this }}
    )
    {% endif %}

QUALIFY ROW_NUMBER() OVER (
        PARTITION BY tx_count_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1