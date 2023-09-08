{{ config(
    materialized = 'incremental',
    unique_key = 'address',
    tags = ['snapshot']
) }}

SELECT
    address,
    NAME,
    about,
    avatar,
    ipfs,
    created_at,
    _inserted_timestamp
FROM
    {{ ref('bronze__snapshot_users') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    AND address NOT IN (
        SELECT
            DISTINCT address
        FROM
            {{ this }}
    )
{% endif %}
