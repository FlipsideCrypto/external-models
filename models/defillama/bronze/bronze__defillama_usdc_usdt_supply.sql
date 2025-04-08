{{ config(
    materialized = 'incremental',
    unique_key = 'protocol_id',
    tags = ['defillama']
) }}

WITH chains AS (

    SELECT
        DISTINCT REPLACE(LOWER(chain), ' ', '-') AS chain
    FROM
        {{ ref('bronze__defillama_chains') }}

),
usdt_supply AS (
    SELECT
        C.chain,
        2 AS stablecoin_id,
        live.udf_api(
            'GET',
            'https://pro-api.llama.fi/{api_key}/stablecoins/stablecoincharts/' || C.chain || '?stablecoin=1',{},{},
            'Vault/prod/external/defillama'
        ) AS READ,
        READ :bytes :: INT AS bytes,
        SYSDATE() AS _inserted_timestamp
    FROM
        chains C
    WHERE
        bytes > 2
),
usdc_supply AS (
    SELECT
        C.chain,
        2 AS stablecoin_id,
        live.udf_api(
            'GET',
            'https://pro-api.llama.fi/{api_key}/stablecoins/stablecoincharts/' || C.chain || '?stablecoin=1',{},{},
            'Vault/prod/external/defillama'
        ) AS READ,
        READ :bytes :: INT AS bytes,
        SYSDATE() AS _inserted_timestamp
    FROM
        chains C
    WHERE
        bytes > 2
)
SELECT
    *
FROM
    usdt_supply
UNION ALL
SELECT
    *
FROM
    usdc_supply
