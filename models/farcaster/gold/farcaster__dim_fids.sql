{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['farcaster'],
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'FARCASTER' } } }
) }}

SELECT
    custody_address,
    fid,
    created_at,
    updated_at
FROM
    {{ source(
        'external_bronze',
        'farcaster_fids'
    ) }}
    qualify(ROW_NUMBER() over (PARTITION BY fid
ORDER BY
    updated_at DESC)) = 1
