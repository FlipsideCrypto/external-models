{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['farcaster'],
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'FARCASTER' }} }
) }}

SELECT
    fid,
    fname,
    display_name,
    avatar_url,
    bio,
    verified_addresses,
    updated_at
FROM
    {{ source(
        'external_bronze',
        'farcaster_profile_with_addresses'
    ) }}
