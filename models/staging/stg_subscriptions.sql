with source as (
    
    select
        id
        , business_id
        , created_at
        , current_term_start
        , current_term_end
        , cancel_schedule_created_at
        , cancelled_at
        , channel
        , country
        , status
        , exchange_rate
        , subscription_plan
    from {{ source('raw', 'raw_subscriptions') }}
)

select *
from source