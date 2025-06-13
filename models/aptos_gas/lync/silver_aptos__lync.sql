{{ config (
    materialized = "incremental",
    unique_key = ['metric','metric_date'],
    tags = ['aptos_gas'],
) }}

WITH base AS (

    SELECT
        metric,
        DATA,
        inserted_timestamp
    FROM
        {{ ref('bronze_aptos__lync_data') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    A.metric,
    b.value :time_stamp :: DATE AS metric_date,
    b.value :count :: bigint AS metric_count,
    b.value :amount :: FLOAT AS metric_amount,
    b.value :amountInUSD :: FLOAT AS metric_amount_in_usd,
    {{ dbt_utils.generate_surrogate_key([ 'metric', 'metric_date']) }} AS lync_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base A,
    LATERAL FLATTEN(
        input => A.data :data :data :result
    ) b qualify ROW_NUMBER() over (
        PARTITION BY A.metric,
        b.value :time_stamp :: DATE
        ORDER BY
            inserted_timestamp DESC
    ) = 1
