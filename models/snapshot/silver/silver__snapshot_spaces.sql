{{ config(
    materialized = 'incremental',
    unique_key = 'space_id',
    incremental_strategy = 'delete+insert',
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
{% endif %}
