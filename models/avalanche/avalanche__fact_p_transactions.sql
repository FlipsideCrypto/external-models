{{ config (
    materialized = "view",
    tags = ['avalanche_share']
) }}

select * 
from {{ source('avalanche_share', 'p_transactions') }}