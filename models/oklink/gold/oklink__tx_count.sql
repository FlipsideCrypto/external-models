-- depends_on: {{ ref('bronze__oklink') }}
{{ config(
    materialized = "incremental",
    unique_key = ['block_date', 'blockchain'],
    tags = ['oklink'],
    enabled = false
) }}

WITH source_data AS (

    SELECT
        date_day :: DATE AS as_of_date,
        blockchain,
        DATA,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref("bronze__oklink") }}
{% else %}
    {{ ref("bronze__oklink_FR") }}
{% endif %}
WHERE
    metric = 'stats'
    AND DATA :code = '0'

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
        _inserted_timestamp,
        TRY_CAST(
            stats.value :totalTransactionCount :: STRING AS INT
        ) AS tx_count
    FROM
        source_data,
        LATERAL FLATTEN(
            input => DATA :data [0] :statsHistoryList
        ) AS stats
    WHERE
        tx_count IS NOT NULL
)
SELECT
    blockchain,
    'tx_count' AS metric,
    'The reported number of totalTransactionCount on the as_of_date' AS description,
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
