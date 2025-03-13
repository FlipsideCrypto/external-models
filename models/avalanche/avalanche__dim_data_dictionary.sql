{{ config (
    materialized = "view",
    tags = ['avalanche_share']
) }}

select * 
from {{ source('avalanche_share', 'data_dictionary') }}