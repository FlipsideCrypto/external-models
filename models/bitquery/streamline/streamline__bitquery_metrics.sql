{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

WITH metrics AS (

    SELECT
        'hedera' AS blockchain,
        'tx_count' AS metric,
        'query ($network: HederaNetwork!, $dateFormat: String!, $from: ISO8601DateTime, $till: ISO8601DateTime) { hedera(network: $network) { transactions(options: {asc: "date.date"}, date: {since: $from, till: $till}) { date {date(format: $dateFormat) } countBigInt}} }' AS query_text,
        'Count of tx hashes by day' AS description
    UNION ALL
    SELECT
        'ripple' AS blockchain,
        'tx_count' AS metric,
        'query ($network: RippleNetwork!, $dateFormat: String!, $from: ISO8601DateTime, $till: ISO8601DateTime) { ripple(network: $network){ transactions(options: {asc: "date.date"}, date: {since: $from, till: $till}) { date: date { date(format: $dateFormat) } countBigInt }} }' AS query_text,
        'Count of tx hashes by day' AS description
    UNION ALL
    SELECT
        'moonbeam' AS blockchain,
        'tx_count' AS metric,
        'query ($network: EthereumNetwork!, $dateFormat: String!, $from: ISO8601DateTime, $till: ISO8601DateTime) { ethereum(network: $network){ transactions(options: {asc: "date.date"}, date: {since: $from, till: $till}) { date: date { date(format: $dateFormat) } countBigInt }} }' AS query_text,
        'Count of tx hashes by day' AS description
    UNION ALL
    SELECT
        'celo_mainnet' AS blockchain,
        'tx_count' AS metric,
        'query ($network: EthereumNetwork!, $dateFormat: String!, $from: ISO8601DateTime, $till: ISO8601DateTime) { ethereum(network: $network){ transactions(options: {asc: "date.date"}, date: {since: $from, till: $till}) { date: date { date(format: $dateFormat) } countBigInt }} }' AS query_text,
        'Count of tx hashes by day' AS description
    UNION ALL
    SELECT
        'algorand' AS blockchain,
        'tx_count' AS metric,
        'query ($network: AlgorandNetwork!, $dateFormat: String!, $from: ISO8601DateTime, $till: ISO8601DateTime) { algorand(network: $network) { transactions(options: {asc: "date.date"}, date: {till: $till, since: $from} ) { date: date { date(format: $dateFormat) } countBigInt }} }' AS query_text,
        'Count of tx hashes by day' AS description
    UNION ALL
    SELECT
        'filecoin' AS blockchain,
        'tx_count' AS metric,
        'query ($network: FilecoinNetwork!, $dateFormat: String!, $from: ISO8601DateTime, $till: ISO8601DateTime) { filecoin(network: $network) { messages(options: {asc: "date.date"}, date: {since: $from, till: $till}) { date: date { date(format: $dateFormat) } countBigInt }} }' AS query_text,
        'Count of messages by day' AS description
    UNION ALL
    SELECT
        'cardano' AS blockchain,
        'tx_count' AS metric,
        'query ($network: CardanoNetwork!, $dateFormat: String!, $from: ISO8601DateTime, $till: ISO8601DateTime) { cardano(network: $network) { transactions(options: {asc: "date.date"}, date: {since: $from, till: $till}) { date: date { date(format: $dateFormat) } countBigInt }} }' AS query_text,
        'Count of messages by day' AS description
    UNION ALL
    SELECT
        'hedera' AS blockchain,
        'active_users' AS metric,
        'query ($network: HederaNetwork!, $from: ISO8601DateTime, $till: ISO8601DateTime) {hedera(network: $network) {transactions(date: {since: $from, till: $till}) { countBigInt(uniq: payer_account) } } }' AS query_text,
        'distinct counts of payer accounts over the last 30 days' AS description
    UNION ALL
    SELECT
        'ripple' AS blockchain,
        'active_users' AS metric,
        'query ($network: RippleNetwork!, $from: ISO8601DateTime, $till: ISO8601DateTime) { ripple(network: $network) { transactions( date: {since: $from, till: $till}) { countBigInt(uniq: senders) } } } ' AS query_text,
        'distinct counts of senders over the last 30 days' AS description
    UNION ALL
    SELECT
        'moonbeam' AS blockchain,
        'active_users' AS metric,
        'query ($network: EthereumNetwork!, $from: ISO8601DateTime, $till: ISO8601DateTime) { ethereum(network: $network){ transactions(date: {since: $from, till: $till}) { countBigInt(uniq: senders) }} }' AS query_text,
        'distinct counts of senders over the last 30 days' AS description
    UNION ALL
    SELECT
        'celo_mainnet' AS blockchain,
        'active_users' AS metric,
        'query ($network: EthereumNetwork!, $from: ISO8601DateTime, $till: ISO8601DateTime) { ethereum(network: $network){ transactions(date: {since: $from, till: $till}) { countBigInt(uniq: senders) }} }' AS query_text,
        'distinct counts of senders over the last 30 days' AS description
    UNION ALL
    SELECT
        'algorand' AS blockchain,
        'active_users' AS metric,
        'query ($network: AlgorandNetwork!, $from: ISO8601DateTime, $till: ISO8601DateTime) { algorand(network: $network) { transactions( date: {since: $from, till: $till} ) { countBigInt(uniq: senders) }} }' AS query_text,
        'distinct counts of senders over the last 30 days' AS description
    UNION ALL
    SELECT
        'filecoin' AS blockchain,
        'active_users' AS metric,
        'query ($network: FilecoinNetwork!, $from: ISO8601DateTime, $till: ISO8601DateTime) { filecoin(network: $network) { messages(date: {since: $from, till: $till}) {senders: countBigInt(uniq: senders) }} }' AS query_text,
        'distinct counts of message senders over the last 30 days' AS description
    UNION ALL
    SELECT
        'cardano' AS blockchain,
        'active_users' AS metric,
        'query ($network: CardanoNetwork!, $from: ISO8601DateTime, $till: ISO8601DateTime) { cardano(network: $network) { activeAddresses(date: {since: $from, till: $till}) { countBigInt(uniq: address) }} }' AS query_text,
        'distinct counts of addresses over the last 30 days' AS description
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
    query_text,
    OBJECT_CONSTRUCT(
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
    ) AS variables,
    description
FROM
    {{ source(
        'crosschain_core',
        'dim_dates'
    ) }}
    CROSS JOIN metrics
WHERE
    date_day >= '2025-01-01'
    AND date_day < SYSDATE() :: DATE
