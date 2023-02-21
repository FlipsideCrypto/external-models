{{ config(
    materialized = 'incremental',
    unique_key = 'api_url',
    full_refresh = false
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
    3
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
)
SELECT
    ethereum.streamline.udf_api(' GET ', api_url, PARSE_JSON(header),{}) AS resp,
    SYSDATE() _inserted_timestamp,
    api_url
FROM
    requests
    JOIN api_key
    ON 1 = 1
WHERE
    EXISTS (
        SELECT
            1
        FROM
            requests
    )
