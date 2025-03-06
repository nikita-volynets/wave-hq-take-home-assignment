WITH source AS (

    SELECT *
    FROM {{ source('raw','raw_subscriptions') }}

),

parsed AS (

    SELECT
        id,
        business_id,
        created_at,
        current_term_start,
        current_term_end,
        cancel_schedule_created_at,
        cancelled_at,
        channel,
        country,
        status,
        exchange_rate,
        -- Converting single quotes to double quotes, then parsing as JSON.
        parse_json(
            replace(subscription_plan, '''', '"')
        ) AS plan_json
    FROM source

)

SELECT
    id as subscription_id,
    business_id,
    created_at as subscription_created_at,
    current_term_start,
    current_term_end,
    cancel_schedule_created_at,
    cancelled_at,
    channel,
    country as subscription_country,
    status as subscription_status,
    exchange_rate,
    plan_json:"item_id"::integer       AS subscription_item_id,
    plan_json:"currency_code"::string AS currency_code

FROM parsed