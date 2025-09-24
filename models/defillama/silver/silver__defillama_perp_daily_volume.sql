-- depends_on: {{ ref('silver__defillama_perp_metrics') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'defillama_perp_daily_volume_id',
    cluster_by = ['date','blockchain','protocol_id'],
    tags = ['defillama']
) }}

with base as (
    select 
        protocol_id,
        slug as protocol_slug,
        name,
        display_name,
        total_data_chart_breakdown,
        timestamp,
        _inserted_timestamp,
        defillama_perp_metrics_id
    from {{ ref('silver__defillama_perp_metrics') }}
    {% if is_incremental() %}
    where _inserted_timestamp > (
        select coalesce(max(_inserted_timestamp), '2025-01-01') from {{ this }}
    )
    {% endif %}
),

-- Flatten the total_data_chart_breakdown to get daily data points
daily_data_flattened as (
    select 
        protocol_id,
        protocol_slug,
        name,
        display_name,
        timestamp,
        _inserted_timestamp,
        defillama_perp_metrics_id,
        daily_date.value[0]::bigint as date_timestamp,
        to_date(to_timestamp(daily_date.value[0]::bigint)) as date_day,
        daily_date.value[1] as chain_breakdown_object,
        daily_date.index as day_index
    from base,
    lateral flatten(input => total_data_chart_breakdown) as daily_date
),

-- Flatten the chain breakdown object to get blockchain/volume pairs
chain_volume_flattened as (
    select 
        ddf.protocol_id,
        ddf.protocol_slug,
        ddf.name,
        ddf.display_name,
        ddf.timestamp,
        ddf._inserted_timestamp,
        ddf.defillama_perp_metrics_id,
        ddf.date_day,
        ddf.date_timestamp,
        ddf.day_index,
        chain_breakdown.key as blockchain,
        chain_breakdown.value as protocol_volumes
    from daily_data_flattened ddf,
    lateral flatten(input => ddf.chain_breakdown_object) as chain_breakdown
),

-- Flatten the protocol volumes to get individual protocol volumes per blockchain
final as (
    select 
        cvf.date_day as date,
        cvf.blockchain,
        cvf.protocol_id,
        cvf.protocol_slug,
        cvf.name as protocol,
        protocol_vol.value::float as volume,
        cvf._inserted_timestamp,
        cvf.defillama_perp_metrics_id,
        {{ dbt_utils.generate_surrogate_key(
            ['cvf.protocol_id','cvf.date_day','cvf.blockchain']
        ) }} as defillama_perp_daily_volume_id,
        sysdate() as inserted_timestamp,
        sysdate() as modified_timestamp,
        '{{ invocation_id }}' as _invocation_id
    from chain_volume_flattened cvf,
    lateral flatten(input => cvf.protocol_volumes) as protocol_vol
)

select * from final
qualify row_number() over (partition by defillama_perp_daily_volume_id order by _inserted_timestamp desc) = 1
