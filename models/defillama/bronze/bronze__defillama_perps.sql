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
    _inserted_timestamp,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    '{{ invocation_id }}' as _invocation_id
FROM
    protocol_expand
