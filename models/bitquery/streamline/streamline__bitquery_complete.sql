-- depends_on: {{ ref('bronze__bitquery_FR') }}
-- depends_on: {{ ref('bronze__bitquery') }}
{{ config (
    materialized = "incremental",
    unique_key = [' date_day','blockchain','metric'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['streamline_realtime'],
    enabled = false
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
{{ ref('bronze__bitquery') }}
{% else %}
    {{ ref('bronze__bitquery_FR') }}
{% endif %}
WHERE
    len(DATA :data) > 10

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
    FROM
        {{ this }})
        AND DATA IS NOT NULL
    {% endif %}

    qualify ROW_NUMBER() over (
        PARTITION BY date_day,
        blockchain,
        metric,
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
