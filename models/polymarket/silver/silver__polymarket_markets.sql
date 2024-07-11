{{ config(
    materialized = 'table',
    unique_key = 'condition_id',
    tags = ['polymarket']
) }}

SELECT
    condition_id,
    question,
    description,
    tokens,
    token_1_token_id,
    token_1_outcome,
    token_1_winner,
    token_2_token_id,
    token_2_outcome,
    token_2_winner,
    enable_order_book,
    active,
    closed,
    archived,
    accepting_orders,
    accepting_order_timestamp,
    minimum_order_size,
    minimum_tick_size,
    question_id,
    market_slug,
    end_date_iso,
    game_start_time,
    seconds_delay,
    fpmm,
    maker_base_fee,
    taker_base_fee,
    notifications_enabled,
    neg_risk,
    neg_risk_market_id,
    neg_risk_request_id,
    rewards,
    tags,
    _inserted_timestamp
FROM
    {{ ref('bronze__polymarket_markets') }} qualify(ROW_NUMBER() over (PARTITION BY condition_id
ORDER BY
    _inserted_timestamp DESC)) = 1