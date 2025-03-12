{{ config (
    materialized = "view",
    tags = ['avalanche_core']
) }}

select * 
from {{ source('avalanche_share', 'utxos') }}