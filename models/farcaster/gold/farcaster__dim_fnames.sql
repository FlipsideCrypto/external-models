{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['farcaster'],
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'FARCASTER' } } }
) }}

SELECT
    fid,
    fname,
    custody_address,
    expires_at,
    created_at,
    updated_at,
    deleted_at
FROM
    {{ source(
        'external_bronze',
        'farcaster_fnames'
    ) }}
    qualify(ROW_NUMBER() over (PARTITION BY fid
ORDER BY
    updated_at DESC)) = 1