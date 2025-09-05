{{ config(
    materialized = 'incremental',
    unique_key = 'defillama_ez_stablecoin_metrics_id',
    cluster_by = ['date_day','chain'],
    tags = ['defillama']
) }}

with base as (
    select 
    replace(lower(s.chain), ' ', '_') as chain,
    s.date_day,
    s.stablecoin_id,
    sc.stablecoin,
    sc.symbol,
    sc.peg_type,
    sc.peg_mechanism,
    GET_PATH(s.total_circulating_usd, sc.peg_type)::float as total_circulating_usd,
    GET_PATH(s.total_minted_usd, sc.peg_type)::float as total_minted_usd,
    GET_PATH(s.total_circulating, sc.peg_type)::float as total_circulating,
    GET_PATH(s.total_bridged_to_usd, sc.peg_type)::float as total_bridged_to_usd,
    GET_PATH(s.total_unreleased, sc.peg_type)::float as total_unreleased,
    run_timestamp
    from 
    {{ ref('silver__defillama_stablecoin_metrics') }} s
    left join {{ ref('bronze__defillama_stablecoins') }} sc using (stablecoin_id)
    {% if is_incremental() %}
    left join {{ this }} t 
    on t.chain = replace(lower(s.chain), ' ', '_') 
    and t.date_day = s.date_day 
    and t.stablecoin_id = s.stablecoin_id
    {% endif %}

    {% if is_incremental() %}
    where s.modified_timestamp > (
        select coalesce(max(modified_timestamp), '2025-01-01') from {{ this }}
    )
    and t.defillama_ez_stablecoin_metrics_id is null -- this is to avoid reloading the same data
    {% endif %}
),
latest_records as (
    select *
    from base
    qualify row_number() over (partition by chain, stablecoin_id, date_day order by run_timestamp desc) = 1
)
select 
    chain,
    date_day,
    stablecoin_id,
    stablecoin,
    symbol,
    peg_type,
    peg_mechanism,
    total_circulating_usd,
    total_minted_usd,
    total_circulating,
    total_bridged_to_usd,
    total_unreleased,
    {{ dbt_utils.generate_surrogate_key(
        ['chain','date_day','stablecoin_id']
    ) }} as ez_stablecoin_metrics_id,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp
from latest_records