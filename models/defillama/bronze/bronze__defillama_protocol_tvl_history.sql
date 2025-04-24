  -- depends_on: {{ ref('bronze__defillama_protocol_historical') }}
  -- depends_on: {{ ref('bronze__defillama_protocol_historical_FR') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['protocol_id', '_inserted_timestamp'],
    cluster_by = ['partition_key'],
    tags = ['defillama']
) }}

WITH protocol_base AS (

    SELECT
        VALUE:PROTOCOL_ID::INT AS protocol_id,
        partition_key,
        VALUE:data:category AS category,
        _inserted_timestamp,
        VALUE:data:chainTvls AS response
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
select 
    protocol_id,
    category,
    partition_key,
    response,
    {{ dbt_utils.generate_surrogate_key(
        ['protocol_id','_inserted_timestamp']
    ) }} AS bronze_defillama_protocol_historical_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
from 
    protocol_base 
