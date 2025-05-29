{{ config (
    materialized = "incremental",
    tags = ['aptos_gas'],
) }}

WITH paths AS (

    SELECT
        *
    FROM
    VALUES
        (
            'total-transactions',
            'Total Transactions'
        ),
        (
            'recurring-users',
            'Total Recurring Users'
        ),
        (
            'retention',
            'Total Retention Users'
        ),
        (
            'gas-consumed',
            'Total Gas Consumed'
        ),
        (
            'accounts-created',
            'Total Accounts Created'
        ),
        (
            'accounts-creation-gas-used',
            'Total Gas Consumed in Account Creation'
        ) AS t(
            suffix,
            metric
        )
)
SELECT
    metric,
    {{ target.database }}.live.udf_api(
        'GET',
        '{Service}/api/v1/analytics/' || suffix || '?period=daily&type=global',
        OBJECT_CONSTRUCT(),
        OBJECT_CONSTRUCT(),
        'Vault/prod/external/aptos/lync'
    ) AS DATA,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    paths
