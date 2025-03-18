{{ config(
    materialized = 'incremental',
    unique_key = 'active_users_id',
    tags = ['oklink']
) }}

WITH source AS (
    SELECT
        date_day::DATE AS as_of_date,
        blockchain,
        data,
        TRY_TO_NUMBER(data:data[0]:activeAddresses) AS active_addresses,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
    {{ ref("bronze_oklink") }}
{% else %}
    {{ ref("bronze_oklink_FR") }}
{% endif %}

    WHERE 
        metric = 'address' 
        AND data:code = '0'
        AND data:data[0]:activeAddresses IS NOT NULL
        AND TRY_TO_NUMBER(data:data[0]:activeAddresses) > 0

{% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT 
            MAX(_inserted_timestamp) 
        FROM 
            {{ this }}
    )
{% endif %}
)

SELECT
    blockchain,
    'active_users' AS metric,
    as_of_date,
    active_addresses as active_users,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['blockchain', 'metric', 'as_of_date']) }} AS active_users_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM source
QUALIFY ROW_NUMBER() 
OVER (
    PARTITION BY 
        active_users_id 
    ORDER BY 
        _inserted_timestamp DESC) = 1