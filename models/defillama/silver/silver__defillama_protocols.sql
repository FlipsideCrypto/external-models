-- depends_on: {{ ref('bronze__defillama_protocols') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'defillama_protocols_id',
    cluster_by = ['run_date'],
    tags = ['defillama']
) }}

SELECT
    TO_TIMESTAMP(VALUE:"RUN_TIMESTAMP"::INT) AS run_timestamp,
    TO_TIMESTAMP(VALUE:"RUN_TIMESTAMP"::INT)::DATE AS run_date,
    DATA:id::STRING AS protocol_id,
    DATA:slug::STRING AS protocol_slug,
    REGEXP_REPLACE(DATA:parentProtocol::STRING, '^parent#', '') AS parent_protocol,
    DATA:name::STRING AS protocol,
    CASE 
        WHEN DATA:address::STRING = '-' THEN NULL 
        ELSE SUBSTRING(LOWER(DATA:address::STRING), CHARINDEX(':', LOWER(DATA:address::STRING))+1) 
    END AS address,
    CASE 
        WHEN DATA:symbol::STRING = '-' THEN NULL 
        ELSE DATA:symbol::STRING 
    END AS symbol,
    DATA:description::STRING AS description,
    DATA:chain::STRING AS chain,
    DATA:audits::INTEGER AS num_audits,
    DATA:audit_note::STRING AS audit_note,
    DATA:category::STRING AS category,
    DATA:url::STRING AS url,
    DATA:logo::STRING AS logo,
    DATA:tvl::FLOAT AS tvl,
    DATA:chains AS chains,
    DATA:chainTvls AS chain_tvls,
    DATA,
    _inserted_timestamp,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    '{{ invocation_id }}' as _invocation_id,
    {{ dbt_utils.generate_surrogate_key(
        ['protocol_id','run_date']
    ) }} AS defillama_protocols_id
    from 
    {% if is_incremental() %}
    {{ ref('bronze__defillama_protocols') }}
    where _inserted_timestamp > (
        select coalesce(max(_inserted_timestamp), '2025-01-01') from {{ this }}
    )
    {% else %}
    {{ ref('bronze__defillama_protocols_FR') }}
    {% endif %}

QUALIFY(
    ROW_NUMBER() OVER (PARTITION BY defillama_protocols_id ORDER BY _inserted_timestamp DESC)
) = 1