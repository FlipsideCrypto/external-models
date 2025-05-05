{{ config(
    materialized = 'table',
    unique_key = ['stablecoin_id','timestamp'],
    tags = ['defillama']
) }}

WITH stablecoin_base AS ({% for item in range(50) %}
    (

    SELECT
        stablecoin_id, 
        stablecoin, 
        symbol, 
        live.udf_api(
            'GET', 
            'https://pro-api.llama.fi/{api_key}/api/stablecoins/stablecoins?includePrices=true'|| stablecoin_id,
            OBJECT_CONSTRUCT(
                'Content-Type', 'text/plain',
                'Accept', 'text/plain'
            ),
            {},
            'Vault/prod/external/defillama'
        ) AS READ, 
        SYSDATE() AS _inserted_timestamp,
    FROM
        (
    SELECT
        stablecoin_id, 
        stablecoin, 
        symbol, 
        row_num
    FROM
        {{ ref('bronze__defillama_stablecoins') }}
    WHERE
        row_num BETWEEN {{ item * 5 + 1 }}
        AND {{(item + 1) * 5 }})) {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %})
SELECT
    READ :data :address :: STRING AS address,
    READ :data :symbol :: STRING AS symbol,
    READ :data :name :: STRING AS NAME,
    READ :data :id :: STRING AS stablecoin_id,
    VALUE AS VALUE,
    TO_TIMESTAMP(
        VALUE :date :: INTEGER
    ) AS TIMESTAMP,
    _inserted_timestamp
FROM
    stablecoin_base,
    LATERAL FLATTEN (
        input => READ :data :tokens
    ) f
