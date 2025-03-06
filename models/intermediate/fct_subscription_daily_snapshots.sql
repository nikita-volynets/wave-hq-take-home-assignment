WITH subscription_ranges AS (

SELECT
    s.subscription_id,
    s.business_id,
    cast(s.subscription_created_at AS date) AS subscription_created_on,
    cast(s.current_term_start AS date) AS current_term_start_on,
    cast(s.current_term_end AS date) AS current_term_end_on,
    cast(s.cancelled_at AS date) AS cancelled_date_on,
    cast(s.cancel_schedule_created_at AS date) AS cancel_schedule_date_on,
    s.subscription_status,
    s.channel,
    s.exchange_rate,
    s.currency_code,
    s.subscription_item_id,
    -- Unifying revenue in USD:
    case 
        when si.billing_period_unit = 'month' 
                then si.unit_price * s.exchange_rate 
        when si.billing_period_unit = 'year' 
                then (si.unit_price * s.exchange_rate) / 12
        else si.unit_price * s.exchange_rate
    END as monthly_recurring_revenue
FROM {{ ref('fct_subscriptions') }} s
INNER JOIN {{ ref('dim_subscription_items') }} si
    ON s.subscription_item_id = si.subscription_item_id

),

calendar_days AS (

    SELECT 
        date_day
    FROM {{ ref('dim_dates') }} 
    WHERE date_day BETWEEN '2020-01-01' AND current_date

),

expanded AS (

    SELECT
        sr.subscription_id,
        sr.business_id,
        c.date_day,
        sr.channel,
        sr.monthly_recurring_revenue,
        CASE
            WHEN c.date_day >= sr.cancelled_date_on THEN 'cancelled'
            ELSE 'active'
        END AS second_status
    FROM subscription_ranges sr
    INNER JOIN calendar_days c
        ON c.date_day BETWEEN sr.subscription_created_on
                          AND coalesce(sr.cancelled_date_on, sr.current_term_end_on, current_date)
)

SELECT * FROM expanded