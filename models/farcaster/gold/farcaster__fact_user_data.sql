{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['farcaster'],
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'FARCASTER' }} }
) }}

SELECT
    id,
    fid,
    TYPE,
    VALUE,
    HASH,
    TIMESTAMP,
    created_at,
    updated_at,
    deleted_at
FROM
    {{ source(
        'external_bronze',
        'farcaster_user_data'
    ) }}
