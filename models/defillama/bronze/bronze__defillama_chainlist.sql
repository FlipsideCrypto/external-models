{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'chain_id',
    full_refresh = false,
    tags = ['defillama']
) }}

WITH chainlist_base AS (

    SELECT
        ethereum.streamline.udf_api(
            'GET',
            'https://chainid.network/chains.json',{},{}
        ) AS READ,
        SYSDATE() AS _inserted_timestamp
),
FINAL AS (
    SELECT
        VALUE :name :: STRING AS chain,
        VALUE :chain :: STRING AS chain_symbol,
        VALUE :icon :: STRING AS icon,
        VALUE :rpc AS rpc,
        VALUE :faucets AS faucets,
        VALUE :nativeCurrency AS native_currency_obj,
        native_currency_obj :name :: STRING AS token_name,
        TRY_TO_NUMBER(
            native_currency_obj :decimals :: STRING
        ) AS token_decimals,
        native_currency_obj :symbol :: STRING AS token_symbol,
        VALUE :infoURL :: STRING AS info_url,
        VALUE :shortName :: STRING AS short_name,
        VALUE :chainId :: STRING AS chain_id,
        VALUE :networkId :: STRING AS network_id,
        VALUE :explorers AS explorers,
        _inserted_timestamp
    FROM
        chainlist_base,
        LATERAL FLATTEN (input => READ :data)

{% if is_incremental() %}
WHERE
    chain_id NOT IN (
        SELECT
            DISTINCT chain_id
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    chain,
    chain_symbol,
    token_name,
    token_decimals,
    token_symbol,
    chain_id,
    network_id,
    icon,
    rpc,
    faucets,
    info_url,
    short_name,
    explorers,
    _inserted_timestamp
FROM
    FINAL
