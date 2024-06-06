{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['farcaster'],
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'FARCASTER' }} }
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
