-- depends_on: {{ ref('bronze__flashbots_protect_txs') }}
{{ config(
    materialized = "incremental",
    unique_key = "_id",
    cluster_by = "round(created_at_block_number,-3)",
    tags = ['flashbots']
) }}

SELECT
    tx_hash,
    LOWER(from_address) AS from_address,
    LOWER(to_address) AS to_address,
    created_at_block_number,
    tx_id,
    hints_selected,
    num_of_builders_shared,
    refund_percent,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'created_at_block_number', 'tx_id']
    ) }} AS _id
FROM

{% if is_incremental() %}
{{ ref('bronze__flashbots_protect_txs') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__flashbots_protect_txs_fr') }}
{% endif %}
