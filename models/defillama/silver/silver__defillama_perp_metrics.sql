-- depends_on: {{ ref('bronze__defillama_perps') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'defillama_perp_metrics_id',
    cluster_by = ['timestamp','protocol_id'],
    tags = ['defillama']
) }}

with base_raw as (
    select 
    data,
    _inserted_timestamp
    from 
    {% if is_incremental() %}
    {{ ref('bronze__defillama_perp_metrics') }}
    where _inserted_timestamp > (
        select coalesce(max(_inserted_timestamp), '2025-01-01') from {{ this }}
    )
    {% else %}
    {{ ref('bronze__defillama_perp_metrics_FR') }}
    {% endif %}
),
base as (
    select 
    _inserted_timestamp :: DATE AS timestamp,
    DATA :defillamaId :: STRING AS protocol_id,
    DATA :category :: STRING AS category,
    DATA :name :: STRING AS name,
    DATA :displayName :: STRING AS display_name,
    DATA :module :: STRING AS module,
    DATA :logo :: STRING AS logo,
    DATA :chains AS chains,
    DATA :protocolType :: STRING AS protocol_type,
    DATA :methodologyURL :: STRING AS methodology_url,
    DATA :methodology AS methodology,
    DATA :parentProtocol :: STRING AS parent_protocol,
    DATA :slug :: STRING AS slug,
    DATA :linkedProtocols AS linked_protocols,
    DATA :total24h :: FLOAT AS total_24h,
    DATA :total48hto24h :: FLOAT AS total_48h_to_24h,
    DATA :total7d :: FLOAT AS total_7d,
    DATA :total30d :: FLOAT AS total_30d,
    DATA :totalAllTime :: FLOAT AS total_all_time,
    DATA :change_1d :: FLOAT AS change_1d,
    DATA :totalDataChartBreakdown AS total_data_chart_breakdown,
    _inserted_timestamp
    from base_raw
)
select 
    timestamp,
    protocol_id,
    category,
    name,
    display_name,
    module,
    logo,
    chains,
    protocol_type,
    methodology_url,
    methodology,
    parent_protocol,
    slug as protocol_slug,
    linked_protocols,
    total_24h,
    total_48h_to_24h,
    total_7d,
    total_30d,
    total_all_time,
    change_1d,
    total_data_chart_breakdown,
    {{ dbt_utils.generate_surrogate_key(
        ['protocol_id','timestamp']
    ) }} as defillama_perp_metrics_id,
    _inserted_timestamp,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    '{{ invocation_id }}' as _invocation_id
from base
qualify row_number() over (partition by defillama_perp_metrics_id order by _inserted_timestamp desc) = 1