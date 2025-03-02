with source as (
    select
        id
        , unit_type
        , billing_period_unit
        , unit_price
    from {{ source('raw', 'raw_subscription_items') }}
)

select *
from source