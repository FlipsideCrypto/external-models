{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['farcaster'],
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'FARCASTER' } } }
) }}

SELECT
    id,
    fid,
    target_fid,
    HASH,
    TIMESTAMP,
    TYPE,
    display_timestamp,
    created_at,
    updated_at,
    deleted_at
FROM
    {{ source(
        'external_bronze',
        'farcaster_links'
    ) }}
    qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    updated_at DESC)) = 1
