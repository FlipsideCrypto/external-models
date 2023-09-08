{{ config(
    materialized = 'incremental',
    unique_key = 'space_id',
    tags = ['snapshot']
) }}

SELECT
    space_id,
    SPACE,
    about,
    symbol,
    network,
    categories,
    domain,
    is_private,
    is_verified,
    admins,
    members,
    treasuries,
    _inserted_timestamp
FROM
    {{ ref('bronze__snapshot_spaces') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    AND space_id NOT IN (
        SELECT
            DISTINCT space_id
        FROM
            {{ this }}
    )
{% endif %}
