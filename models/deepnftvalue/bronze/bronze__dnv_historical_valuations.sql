{{ config(
    materialized = 'incremental',
    unique_key = 'api_url',
    full_refresh = false,
    enabled = false
) }}

WITH requests AS (

    SELECT
        api_url,
        date_day
    FROM
        {{ ref('silver__dnv_historical_requests') }}

{% if is_incremental() %}
WHERE
    api_url NOT IN (
        SELECT
            api_url
        FROM
            {{ this }}
    )
{% endif %}
ORDER BY
    date_day DESC
LIMIT
    30
), api_key AS (
    SELECT
        CONCAT(
            '{\'Authorization\': \'Token ',
            api_key,
            '\', \'accept\': \'application/json\'}'
        ) AS header
    FROM
        {{ source(
            'crosschain_silver',
            'apis_keys'
        ) }}
    WHERE
        api_name = 'deepnftvalue'
),
row_nos AS (
    SELECT
        api_url,
        ROW_NUMBER () over (
            ORDER BY
                api_url
        ) AS row_no,
        FLOOR(
            row_no / 2
        ) AS batch_no,
        header
    FROM
        requests
        JOIN api_key
        ON 1 = 1
),
batched AS ({% for item in range(10) %}
SELECT
    ethereum.streamline.udf_api(' GET ', api_url, PARSE_JSON(header),{}) AS resp, api_url, SYSDATE() _inserted_timestamp
FROM
    row_nos rn
WHERE
    batch_no = {{ item }}
    AND EXISTS (
SELECT
    1
FROM
    row_nos
WHERE
    batch_no = {{ item }}
LIMIT
    1) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %})
SELECT
    resp,
    _inserted_timestamp,
    api_url
FROM
    batched
