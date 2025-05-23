-- depends_on: {{ ref('bronze__oklink_FR') }}
-- depends_on: {{ ref('bronze__oklink') }}
{{ config (
    materialized = "incremental",
    unique_key = [' date_day','blockchain','metric'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['streamline_realtime']
) }}

SELECT
    date_day,
    blockchain,
    metric,
    partition_key,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    file_name,
    '{{ invocation_id }}' AS _invocation_id,
FROM

{% if is_incremental() %}
{{ ref('bronze__oklink') }}
{% else %}
    {{ ref('bronze__oklink_FR') }}
{% endif %}
WHERE
    DATA :errors IS NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
    FROM
        {{ this }})
    {% endif %}

    qualify ROW_NUMBER() over (
        PARTITION BY date_day,
        blockchain,
        metric
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
