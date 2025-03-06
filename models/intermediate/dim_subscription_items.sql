WITH final AS (

    SELECT *
    FROM {{ ref('stg_subscription_items') }}
)

SELECT
    subscription_item_id,
    unit_type,
    billing_period_unit,
    unit_price
FROM final
