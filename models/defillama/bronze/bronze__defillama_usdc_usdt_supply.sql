{{ config(
    materialized = 'incremental',
    unique_key = ['chain', 'stablecoin_id','_inserted_timestamp'],
    tags = ['defillama']
) }}

WITH usdt_supply AS (

    SELECT
        C.chain,
        1 AS stablecoin_id,
        live.udf_api(
            'GET',
            'https://pro-api.llama.fi/{api_key}/stablecoins/stablecoincharts/' || C.chain || '?stablecoin=1',{},{},
            --usdt
            'Vault/prod/external/defillama'
        ) AS READ,
        SYSDATE() AS _inserted_timestamp
    FROM
        {{ ref('bronze__defillama_usdt_usdc_chain_seed') }} C
    WHERE
        stablecoin_id = 1
),
usdc_supply AS (
    SELECT
        C.chain,
        2 AS stablecoin_id,
        live.udf_api(
            'GET',
            'https://pro-api.llama.fi/{api_key}/stablecoins/stablecoincharts/' || C.chain || '?stablecoin=2',{},{},
            --usdc
            'Vault/prod/external/defillama'
        ) AS READ,
        SYSDATE() AS _inserted_timestamp
    FROM
        {{ ref('bronze__defillama_usdt_usdc_chain_seed') }} C
    WHERE
        stablecoin_id = 2
)
SELECT
    chain,
    stablecoin_id,
    READ,
    bytes,
    _inserted_timestamp
FROM
    usdt_supply
UNION ALL
SELECT
    chain,
    stablecoin_id,
    READ,
    bytes,
    _inserted_timestamp
FROM
    usdc_supply
