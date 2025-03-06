WITH final AS (
	
	SELECT *
	FROM {{ ref('stg_subscriptions') }}

)

SELECT
    subscription_id,
    business_id,
    subscription_created_at,
    current_term_start,
    current_term_end,
    cancel_schedule_created_at,
    cancelled_at,
    channel,
    subscription_country,
    subscription_status,
    exchange_rate,
    subscription_item_id,
    currency_code
FROM final