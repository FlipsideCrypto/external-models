{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

WITH metrics AS (

    SELECT
        'hedera' AS blockchain,
        'tx_count' AS metric,
        'query ($network: HederaNetwork!, $dateFormat: String!, $from: ISO8601DateTime, $till: ISO8601DateTime) {hedera(network: $network) {transactions(options: {asc: "date.date"}, date: {since: $from, till: $till}) {date {date(format: $dateFormat)} count: countBigInt}}}' AS query_text
    UNION ALL
    SELECT
        'ripple' AS blockchain,
        'tx_count' AS metric,
        '' AS query_text
    UNION ALL
        --idk how we're defining active users - thinking it will just be the unique count over the last 30 days
    SELECT
        'hedera' AS blockchain,
        'active_users' AS metric,
        'query ($network: HederaNetwork!, $dateFormat: String!, $from: ISO8601DateTime, $till: ISO8601DateTime) {hedera(network: $network) {transactions(options: {asc: "date.date"}, date: {since: $from, till: $till}) count: countBigInt(uniq: payer_account)}}}' AS query_text {# UNION ALL
    SELECT
        'ripple' AS blockchain,
        'active_users' AS metric,
        '' AS query_text #}
)
SELECT
    date_day,
    DATEADD(
        'day',
        -30,
        date_day
    ) AS date_day_minus_30,
    blockchain,
    metric,
    CASE
        WHEN blockchain = 'ripple'
        AND metric = 'tx_count' THEN '{ripple(network: ripple) {transactions(date: {after: "' || date_day || '"}) {countBigInt(hash: {}, date: {after: "' || date_day || '"}) date {date}}}}'
        WHEN blockchain = 'ripple'
        AND metric = 'active_users' THEN '{ripple(network: ripple) {transactions(date: {after: "' || date_day_minus_30 || '"}) {countBigInt(date: {after: "' || date_day_minus_30 || '"}, uniq: senders) }}}'
        ELSE query_text
    END AS query_text,
    CASE
        WHEN blockchain = 'hedera' THEN OBJECT_CONSTRUCT(
            'limit',
            '1',
            'offset',
            '0',
            'network',
            blockchain,
            'from',
            CASE
                WHEN metric = 'active_users' THEN date_day_minus_30
                ELSE date_day
            END,
            'till',
            date_day,
            'dateFormat',
            '%Y-%m-%d'
        )
    END AS variables
FROM
    {{ source(
        'crosschain_core',
        'dim_dates'
    ) }}
    CROSS JOIN metrics
WHERE
    date_day >= '2025-01-01'
    AND date_day < SYSDATE() :: DATE
