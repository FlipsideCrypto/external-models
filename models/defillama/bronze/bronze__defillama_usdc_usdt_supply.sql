{{ config(
    materialized = 'incremental',
    unique_key = ['chain', 'stablecoin_id','_inserted_timestamp'],
    tags = ['defillama']
) }}

WITH chains AS (

    SELECT
        DISTINCT REPLACE(LOWER(chain), ' ', '-') AS chain
    FROM
        {{ ref('bronze__defillama_chains') }}
    WHERE REPLACE(LOWER(chain), ' ', '-') IN (
        'algorand',
        'aptos',
        'arbitrum',
        'astar',
        'aurora',
        'avalanche',
        'berachain',
        'binance',
        'bittorrent',
        'boba',
        'canto',
        'celo',
        'crab',
        'defichain',
        'dogechain',
        'elrond',
        'eos',
        'ethereum',
        'ethereumclassic',
        'everscale',
        'evmos',
        'fantom',
        'fuse',
        'harmony',
        'heco',
        'icp',
        'injective',
        'ink',
        'iotex',
        'kardia',
        'kava',
        'klaytn',
        'kucoin',
        'kusama',
        'linea',
        'manta',
        'mantle',
        'meter',
        'metis',
        'mixin',
        'mode',
        'moonbeam',
        'moonriver',
        'morph',
        'near',
        'neo',
        'oasis',
        'okexchain',
        'ontology',
        'optimism',
        'osmosis',
        'polkadot',
        'pulse',
        'pulsechain',
        'rsk',
        'scroll',
        'sei',
        'shiden',
        'solana',
        'stacks',
        'starknet',
        'sui',
        'sxnetwork',
        'syscoin',
        'taiko',
        'telos',
        'terra',
        'tezos',
        'theta',
        'thundercore',
        'tomochain',
        'ton',
        'tron',
        'wanchain',
        'waves',
        'xdai',
        'zilliqa',
        'zircuit'
    )
),
usdt_supply AS (
    SELECT
        C.chain,
        1 AS stablecoin_id,
        live.udf_api(
            'GET',
            'https://pro-api.llama.fi/{api_key}/stablecoins/stablecoincharts/' || C.chain || '?stablecoin=1',{},{},
            --usdt
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
            'https://pro-api.llama.fi/{api_key}/stablecoins/stablecoincharts/' || C.chain || '?stablecoin=2',{},{},
            --usdc
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
    chain,
    stablecoin_id,
    read,
    bytes,
    _inserted_timestamp
FROM
    usdt_supply
UNION ALL
SELECT
    chain,
    stablecoin_id,
    read,
    bytes,
    _inserted_timestamp
FROM
    usdc_supply
