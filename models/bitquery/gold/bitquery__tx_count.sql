-- depends_on: {{ ref('bronze__bitquery') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['blockchain', 'metric', 'block_date'],
    tags = ['bitquery']
) }}

WITH base AS (

    SELECT
        A.blockchain,
        A.metric,
        REGEXP_SUBSTR(
            A.data,
            '"date"\\s*:\\s*\\{\\s*"date"\\s*:\\s*"([^"]+)"',
            1,
            1,
            'e',
            1
        ) AS block_date,
        REGEXP_SUBSTR(
            A.data,
            '"countBigInt"\\s*:\\s*"([^"]+)"',
            1,
            1,
            'e',
            1
        ) AS tx_count,
        A._inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__bitquery') }}
{% else %}
    {{ ref('bronze__bitquery_FR') }}
{% endif %}

A
WHERE
    A.data :errors IS NULL
    AND A.metric = 'tx_count'
    AND tx_count IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    blockchain,
    metric,
    block_date,
    tx_count,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['blockchain','metric','block_date']
    ) }} AS accounts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base qualify ROW_NUMBER() over (
        PARTITION BY blockchain,
        metric,
        block_date
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
