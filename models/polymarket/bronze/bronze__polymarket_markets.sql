{{ config(
    materialized = 'table',
    unique_key = 'question_id',
    tags = ['polymarket']
) }}

with recursive api_results as (
    -- Initial request without cursor
    select
        null as cursor,
        live.udf_api(
            'GET', 
            'https://clob.polymarket.com/markets?limit=100',
            {},
            {}
        ) as read
    union all
    -- Recursive step to handle pagination using cursor
    select
        parse_json(read):next_cursor as cursor,
        live.udf_api(
            'GET',
            'https://clob.polymarket.com/markets?limit=100&next_cursor=' || parse_json(read:data):next_cursor,
            {},
            {}
        ) as read
    from api_results
    where parse_json(read:data):next_cursor is not null
),
FINAL AS (
    SELECT
        VALUE :condition_id::STRING AS condition_id,
        VALUE :question::STRING AS question,
        VALUE :description::STRING AS description,
        VALUE :tokens AS tokens,
        VALUE :tokens[0]:token_id::STRING AS token_1_token_id,
        VALUE :tokens[0]:outcome::STRING AS token_1_outcome,
        VALUE :tokens[0]:winner::STRING AS token_1_winner,
        VALUE :tokens[1]:token_id::STRING AS token_2_token_id,
        VALUE :tokens[1]:outcome::STRING AS token_2_outcome,
        VALUE :tokens[1]:winner::STRING AS token_2_winner,
        VALUE :enable_order_book::STRING AS enable_order_book,
        VALUE :active::BOOLEAN AS active,
        VALUE :closed::BOOLEAN AS closed,
        VALUE :archived::BOOLEAN AS archived,
        VALUE :accepting_orders::BOOLEAN AS accepting_orders,
        VALUE :accepting_order_timestamp::TIMESTAMP AS accepting_order_timestamp,
        VALUE :minimum_order_size::INTEGER AS minimum_order_size,
        VALUE :minimum_tick_size::INTEGER AS minimum_tick_size,
        VALUE :question_id::STRING AS question_id,
        VALUE :market_slug::STRING AS market_slug,
        VALUE :end_date_iso::TIMESTAMP AS end_date_iso,
        VALUE :game_start_time::TIMESTAMP AS game_start_time,
        VALUE :seconds_delay::INTEGER AS seconds_delay,
        VALUE :fpmm::STRING AS fpmm,
        VALUE :maker_base_fee::INTEGER AS maker_base_fee,
        VALUE :taker_base_fee::INTEGER AS taker_base_fee,
        VALUE :notifications_enabled::BOOLEAN AS notifications_enabled,
        VALUE :neg_risk::BOOLEAN AS neg_risk,
        VALUE :neg_risk_market_id::STRING AS neg_risk_market_id,
        VALUE :neg_risk_request_id::STRING AS neg_risk_request_id,
        VALUE :rewards::VARIANT AS rewards,
        VALUE :tags::VARIANT AS tags,
        SYSDATE() AS _inserted_timestamp
    FROM
        api_results,
        LATERAL FLATTEN (
            input => READ :data :data
        ) f
)
SELECT
    *
FROM
    FINAL
WHERE condition_id <> ''