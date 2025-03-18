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
        TRY_TO_NUMBER(data:data[0]:activeAddresses::STRING) AS active_addresses,
        TRY_TO_NUMBER(data:data[0]:newActiveAddresses::STRING) AS active_addresses_change,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
    {{ ref("bronze__oklink") }}
{% else %}
    {{ ref("bronze__oklink_FR") }}
{% endif %}

    WHERE 
        metric = 'address' 
        AND data:code = '0'
        AND data:data[0]:activeAddresses IS NOT NULL
        AND TRY_TO_NUMBER(data:data[0]:activeAddresses::STRING) > 0
),

{% if is_incremental() %}
prev_data AS (
    -- Get all existing data to ensure complete history
    SELECT
        blockchain,
        as_of_date,
        active_users,
        active_users_change,
        active_users_reported,
        _inserted_timestamp
    FROM {{ this }}
),
combined_data AS (
    -- New data from API
    SELECT
        s.blockchain,
        s.as_of_date,
        s.active_addresses AS active_users_reported,
        s.active_addresses_change AS active_users_change,
        s._inserted_timestamp,
        TRUE AS is_new_data
    FROM source s

    UNION ALL
    
    SELECT
        p.blockchain,
        p.as_of_date,
        p.active_users_reported,
        p.active_users_change,
        p._inserted_timestamp,
        FALSE AS is_new_data
    FROM prev_data p
    WHERE NOT EXISTS (
        SELECT 1 
        FROM source s 
        WHERE s.blockchain = p.blockchain 
        AND s.as_of_date = p.as_of_date
    )
),

{% else %}
combined_data AS (
    -- Initial load/non-incremental - just use source data
    SELECT
        blockchain,
        as_of_date,
        active_addresses AS active_users_reported,
        active_addresses_change AS active_users_change,
        _inserted_timestamp,
        TRUE AS is_new_data
    FROM source
),
{% endif %}

blockchain_first_date AS (
    SELECT
        blockchain,
        MIN(as_of_date) AS first_date
    FROM combined_data
    GROUP BY 1
),
calculations AS (
    SELECT
        cd.blockchain,
        cd.as_of_date,
        cd.active_users_reported,
        COALESCE(cd.active_users_change, 0) AS active_users_change,
        cd._inserted_timestamp,
        cd.is_new_data,
        bfd.first_date,
        CASE WHEN cd.as_of_date = bfd.first_date THEN cd.active_users_reported ELSE NULL END AS base_value
    FROM
        combined_data cd
    JOIN blockchain_first_date bfd
        ON cd.blockchain = bfd.blockchain
),
final AS (
    SELECT
        blockchain,
        as_of_date,
        active_users_reported,
        active_users_change,
        _inserted_timestamp,
        -- Calculate our own cumulative value: starting with base_value on first date -> add the changes to running sum
        SUM(COALESCE(base_value, 0)) OVER (
            PARTITION BY blockchain 
            ORDER BY as_of_date
            ROWS UNBOUNDED PRECEDING
        ) +
        -- Add all changes except on the first day (since base_value includes all previous activity)
        SUM(CASE WHEN base_value IS NULL THEN active_users_change ELSE 0 END) OVER (
            PARTITION BY blockchain 
            ORDER BY as_of_date
            ROWS UNBOUNDED PRECEDING
        ) AS active_users
    FROM calculations
)       

SELECT
    blockchain,
    'active_users' AS metric,
    as_of_date,
    active_users,
    active_users_change,
    active_users_reported,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['blockchain', 'metric', 'as_of_date']) }} AS active_users_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    final
QUALIFY ROW_NUMBER() 
OVER (
    PARTITION BY 
        blockchain, as_of_date
    ORDER BY 
        _inserted_timestamp DESC NULLS LAST) = 1