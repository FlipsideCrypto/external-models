-- depends_on: {{ ref('bronze__defillama_protocol_historical_FR') }}
-- depends_on: {{ ref('bronze__defillama_protocol_historical') }}
{{ config (
    materialized = "incremental",
    unique_key = ['protocol_id','_inserted_timestamp'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['streamline_realtime']
) }}

WITH complete_data AS (

    SELECT
        VALUE:PROTOCOL_ID::INT AS protocol_id,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__defillama_protocol_historical') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            max(_inserted_timestamp)
        FROM
            {{ this }}
        )
        {% else %}
            {{ ref('bronze__defillama_protocol_historical_FR') }}
        {% endif %}
)
SELECT
    protocol_id,
    {{ dbt_utils.generate_surrogate_key(
        ['protocol_id','_inserted_timestamp']
    ) }} AS complete_defillama_protocol_historical_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    complete_data qualify(ROW_NUMBER() over (PARTITION BY protocol_id
ORDER BY
    _inserted_timestamp DESC)) = 1