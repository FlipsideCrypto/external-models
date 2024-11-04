{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['farcaster'],
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'FARCASTER' } } }
) }}

SELECT
    blocker_fid,
    blocked_fid,
    id,
    created_at,
    deleted_at
FROM
    {{ source(
        'external_bronze',
        'farcaster_blocks'
    ) }}
    qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    created_at DESC)) = 1
