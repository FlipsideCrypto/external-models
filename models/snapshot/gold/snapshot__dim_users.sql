{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'SNAPSHOT',
    'PURPOSE': 'GOVERNANCE' } } }
) }}

SELECT
    address,
    NAME,
    about,
    avatar,
    ipfs,
    created_at
FROM
    {{ ref('silver__snapshot_users') }}
