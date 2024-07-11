{{ config(
    materialized = 'incremental',
    unique_key = 'question_id',
    tags = ['polymarket']
) }}

WITH recursive api_results AS (

    SELECT
        NULL AS CURSOR,
        live.udf_api(
            'GET',
            'https://clob.polymarket.com/markets?limit=100',{},{}
        ) AS READ
    UNION ALL
        -- Recursive step to handle pagination using cursor
    SELECT
        PARSE_JSON(READ) :next_cursor AS CURSOR,
        live.udf_api(
            'GET',
            'https://clob.polymarket.com/markets?limit=100&next_cursor=' || PARSE_JSON(
                READ :data
            ) :next_cursor,{},{}
        ) AS READ
    FROM
        api_results
    WHERE
        PARSE_JSON(
            READ :data
        ) :next_cursor IS NOT NULL
),
FINAL AS (
    SELECT
        VALUE :condition_id :: STRING AS condition_id,
        VALUE :question :: STRING AS question,
        VALUE :description :: STRING AS description,
        VALUE :tokens AS tokens,
        VALUE :tokens [0] :token_id :: STRING AS token_1_token_id,
        VALUE :tokens [0] :outcome :: STRING AS token_1_outcome,
        VALUE :tokens [0] :winner :: STRING AS token_1_winner,
        VALUE :tokens [1] :token_id :: STRING AS token_2_token_id,
        VALUE :tokens [1] :outcome :: STRING AS token_2_outcome,
        VALUE :tokens [1] :winner :: STRING AS token_2_winner,
        VALUE :enable_order_book :: STRING AS enable_order_book,
        VALUE :active :: BOOLEAN AS active,
        VALUE :closed :: BOOLEAN AS closed,
        VALUE :archived :: BOOLEAN AS archived,
        VALUE :accepting_orders :: BOOLEAN AS accepting_orders,
        VALUE :accepting_order_timestamp :: TIMESTAMP AS accepting_order_timestamp,
        VALUE :minimum_order_size :: INTEGER AS minimum_order_size,
        VALUE :minimum_tick_size :: INTEGER AS minimum_tick_size,
        VALUE :question_id :: STRING AS question_id,
        VALUE :market_slug :: STRING AS market_slug,
        VALUE :end_date_iso :: TIMESTAMP AS end_date_iso,
        VALUE :game_start_time :: TIMESTAMP AS game_start_time,
        VALUE :seconds_delay :: INTEGER AS seconds_delay,
        VALUE :fpmm :: STRING AS fpmm,
        VALUE :maker_base_fee :: INTEGER AS maker_base_fee,
        VALUE :taker_base_fee :: INTEGER AS taker_base_fee,
        VALUE :notifications_enabled :: BOOLEAN AS notifications_enabled,
        VALUE :neg_risk :: BOOLEAN AS neg_risk,
        VALUE :neg_risk_market_id :: STRING AS neg_risk_market_id,
        VALUE :neg_risk_request_id :: STRING AS neg_risk_request_id,
        VALUE :rewards :: variant AS rewards,
        VALUE :tags :: variant AS tags,
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
WHERE
    condition_id <> '' AND token_1_token_id <> ''
{% if is_incremental() %}
AND condition_id NOT IN (
    SELECT
        DISTINCT condition_id
    FROM
        {{ this }}
)
{% endif %}
