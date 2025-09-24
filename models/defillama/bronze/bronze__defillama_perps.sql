{{ config(
    materialized = 'incremental',
    unique_key = ['protocol_id'],
    cluster_by = ['protocol_id'],
    tags = ['defillama']
) }}

WITH api_pull AS (

    SELECT
        PARSE_JSON(
            live.udf_api(
                'GET',
                'https://pro-api.llama.fi/{api_key}/api/overview/derivatives?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true',
                {},
                {},
                'Vault/prod/external/defillama'
            )
        ) :data:protocols AS response,
        SYSDATE() AS _inserted_timestamp
),
lat_flat AS (
    SELECT
        r.value AS VALUE,
        _inserted_timestamp
    FROM
        api_pull,
        LATERAL FLATTEN (response) AS r
),
protocol_expand AS (
    SELECT
        VALUE :defillamaId :: STRING AS protocol_id,
        VALUE :category :: STRING AS category,
        VALUE :name :: STRING AS NAME,
        VALUE :displayName :: STRING AS display_name,
        VALUE :module :: STRING AS module,
        VALUE :logo :: STRING AS logo,
        VALUE :chains AS chains,
        VALUE :protocolType :: STRING AS protocol_type,
        VALUE :methodologyURL :: STRING AS methodology_url,
        VALUE :methodology AS methodology,
        VALUE :parentProtocol :: STRING AS parent_protocol,
        VALUE :slug :: STRING AS slug,
        VALUE :linkedProtocols AS linked_protocols,
        VALUE :total24h :: FLOAT AS total_24h,
        VALUE :total48hto24h :: FLOAT AS total_48h_to_24h,
        VALUE :total7d :: FLOAT AS total_7d,
        VALUE :total14dto7d :: FLOAT AS total_14d_to_7d,
        VALUE :total30d :: FLOAT AS total_30d,
        VALUE :total60dto30d :: FLOAT AS total_60d_to_30d,
        VALUE :total1y :: FLOAT AS total_1y,
        VALUE :totalAllTime :: FLOAT AS total_all_time,
        VALUE :average1y :: FLOAT AS average_1y,
        VALUE :monthlyAverage1y :: FLOAT AS monthly_average_1y,
        VALUE :total7DaysAgo :: FLOAT AS total_7_days_ago,
        VALUE :total30DaysAgo :: FLOAT AS total_30_days_ago,
        VALUE :breakdown24h AS breakdown_24h,
        VALUE :breakdown30d AS breakdown_30d,
        _inserted_timestamp
    FROM
        lat_flat
    {% if is_incremental() %}
    where VALUE :defillamaId :: STRING NOT IN (
        select protocol_id from {{ this }}
    )
    {% endif %}
)
SELECT
    protocol_id,
    slug as protocol_slug,
    category,
    NAME,
    display_name,
    module,
    logo,
    chains,
    protocol_type,
    methodology_url,
    methodology,
    parent_protocol,
    linked_protocols,
    _inserted_timestamp
FROM
    protocol_expand
