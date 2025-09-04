-- depends_on: {{ ref('bronze__defillama_stablecoin_metrics') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'defillama_stablecoin_metrics_id',
    cluster_by = ['date_day','chain'],
    tags = ['defillama']
) }}

with base as (
    select 
    value:"CHAIN"::string as chain,
    value:"STABLECOIN_ID"::string as stablecoin_id,
    to_timestamp(value:"RUN_TIMESTAMP"::string) as run_timestamp,
    partition_key,
    data:totalCirculatingUSD as total_circulating_usd,
    data:totalMintedUSD as total_minted_usd,
    data:totalCirculating as total_circulating,
    data:totalBridgedToUSD as total_bridged_to_usd,
    data:totalUnreleased as total_unreleased,
    to_date(data:date::string) as date_day,
    _inserted_timestamp
    from 
    {% if is_incremental() %}
    {{ ref('bronze__defillama_stablecoin_metrics') }}
    where _inserted_timestamp > (
        select coalesce(max(_inserted_timestamp), '2025-01-01') from {{ this }}
    )
    {% else %}
    {{ ref('bronze__defillama_stablecoin_metrics_FR') }}
    {% endif %}
)
select 
chain,
date_day,
stablecoin_id,
total_circulating_usd,
total_minted_usd,
total_circulating,
total_bridged_to_usd,
total_unreleased,
run_timestamp,
partition_key,
_inserted_timestamp,
{{ dbt_utils.generate_surrogate_key(
    ['chain','stablecoin_id','date_day', 'run_timestamp']
) }} as defillama_stablecoin_metrics_id,
sysdate() as inserted_timestamp,
sysdate() as modified_timestamp,
'{{ invocation_id }}' as _invocation_id
from base 
qualify row_number() over (partition by defillama_stablecoin_metrics_id order by _inserted_timestamp desc) = 1