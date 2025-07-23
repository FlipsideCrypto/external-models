{{ config(
    materialized = 'table',
    tags = ['defillama']
) }}

WITH investor_deals AS (
    -- Flatten lead investors
    SELECT 
        TRIM(lead_investor.value::STRING) AS investor,
        funding_date,
        project_name,
        funding_round,
        amount_raised,
        chains,
        category,
        raise_id,
        _inserted_timestamp
    FROM {{ ref('silver__defillama_raises') }} r,
    LATERAL FLATTEN(input => r.lead_investors) AS lead_investor
    WHERE investor IS NOT NULL 
    AND investor != ''
    
    UNION ALL
    
    -- Flatten other investors
    SELECT 
        TRIM(other_investor.value::STRING) AS investor,
        funding_date,
        project_name,
        funding_round,
        amount_raised,
        chains,
        category,
        raise_id,
        _inserted_timestamp
    FROM {{ ref('silver__defillama_raises') }} r,
    LATERAL FLATTEN(input => r.other_investors) AS other_investor
    WHERE investor IS NOT NULL 
    AND investor != ''
),

investor_chains AS (
    -- Flatten chains for each investor deal
    SELECT 
        investor,
        funding_date,
        project_name,
        funding_round,
        amount_raised,
        TRIM(chains.value::STRING) AS chain,
        category,
        raise_id,
        _inserted_timestamp
    FROM investor_deals d,
    LATERAL FLATTEN(input => d.chains) AS chains
    WHERE chain IS NOT NULL 
    AND chain != ''
),

investor_aggregates AS (
    SELECT 
        investor,
        COUNT(DISTINCT raise_id) AS deals,
        SUM(amount_raised) AS total_amount,
        MEDIAN(amount_raised) AS median_amount,
        ARRAY_AGG(DISTINCT chain) AS chains,
        ARRAY_AGG(DISTINCT project_name) AS projects,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM investor_chains
    GROUP BY investor
),

top_deals AS (
    SELECT 
        investor,
        category AS top_project_category,
        funding_round AS top_round_type,
        ROW_NUMBER() OVER (
            PARTITION BY investor 
            ORDER BY amount_raised DESC, funding_date DESC
        ) AS rn
    FROM investor_deals
),

investor_metrics AS (
    SELECT 
        a.investor,
        a.deals,
        a.total_amount,
        a.median_amount,
        a.chains,
        a.projects,
        t.top_project_category,
        t.top_round_type,
        a._inserted_timestamp
    FROM investor_aggregates a
    LEFT JOIN top_deals t 
        ON a.investor = t.investor 
        AND t.rn = 1
)

SELECT 
    investor,
    deals,
    total_amount,
    median_amount,
    chains,
    top_project_category,
    top_round_type,
    projects,
    {{ dbt_utils.generate_surrogate_key(['investor']) }} AS defillama_investors_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM investor_metrics

QUALIFY(
    ROW_NUMBER() OVER (PARTITION BY investor ORDER BY _inserted_timestamp DESC)
) = 1 