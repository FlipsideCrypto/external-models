{{ config (
    materialized = "view",
    tags = ['aptos_gas'],
) }}

SELECT
    TO_DATE (
        file_date :: STRING,
        'YYYYMMDD'
    ) AS metric_date,
    fund_name,
    workspace_name,
    contact_email,
    fund_balance_remaining :: bigint AS fund_balance_remaining,
    total_sponsorships :: bigint AS total_sponsorships,
    total_fees :: bigint AS total_fees
FROM
    {{ source(
        'external_bronze',
        'aptos_shinam'
    ) }}
