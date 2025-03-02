with source as (

select
    id
    , create_date
    , country
    , organizational_type
    , type
    , subtype
from {{ source('raw', 'raw_business') }}

)

select *
from source
