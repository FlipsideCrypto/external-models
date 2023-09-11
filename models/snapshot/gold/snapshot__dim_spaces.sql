{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'SNAPSHOT',
    'PURPOSE': 'GOVERNANCE' } } },
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
    treasuries
FROM
    {{ ref('silver__snapshot_spaces') }}
