-- depends_on: {{ ref('bronze__flashbots_mev_txs') }}
{{ config(
    materialized = "incremental",
    unique_key = "_id",
    cluster_by = "round(block_number,-3)",
    tags = ['flashbots']
) }}

SELECT
    block_number,
    block_time,
    block_hash,
    extra_data,
    fee_recipient_address,
    bundle_id,
    user_tx_hash,
    LOWER(user_tx_from) AS user_tx_from,
    LOWER(user_tx_to) AS user_tx_to,
    backrun_tx_hash,
    LOWER(backrun_tx_from) AS backrun_tx_from,
    LOWER(backrun_tx_to) AS backrun_tx_to,
    refund_tx_hash,
    LOWER(refund_from) AS refund_from,
    LOWER(refund_to) AS refund_to,
    refund_value_eth,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['user_tx_hash', 'backrun_tx_hash', 'bundle_id']
    ) }} AS _id
FROM

{% if is_incremental() %}
{{ ref('bronze__flashbots_mev_txs') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__flashbots_mev_txs_fr') }}
{% endif %}
