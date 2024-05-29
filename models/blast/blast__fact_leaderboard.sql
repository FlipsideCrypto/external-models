{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['blast'],
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'BLAST'
            }
        }
    }
) }}

SELECT
    DATE,
    leaderboard_type,
    RANK,
    NAME,
    points_amount,
    gold_amount,
    invited_by
FROM 
    {{ ref('bronze__blast_leaderboard') }}