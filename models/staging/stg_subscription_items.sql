WITH source AS (
    SELECT *
    FROM {{ source('raw', 'raw_subscription_items') }}
)

SELECT
    id as subscription_item_id,
    unit_type,
    billing_period_unit,
    unit_price
FROM source