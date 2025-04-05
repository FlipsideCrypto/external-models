-- depends_on: {{ ref("bronze__artemis")}}
-- depends_on: {{ ref("bronze__artemis_FR")}}
{{ config (
    materialized = "incremental",
    unique_key = ['date_day'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['streamline_realtime']
) }}

SELECT
    date_day,
    data,
    partition_key,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    file_name,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__artemis') }}
{% else %}
    {{ ref('bronze__artemis_FR') }}
{% endif %}
WHERE
    DATA IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
    FROM
        {{ this }})
    {% endif %}

    qualify ROW_NUMBER() over (
        PARTITION BY date_day
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
