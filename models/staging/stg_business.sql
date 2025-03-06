WITH source AS (

SELECT *
FROM {{ source('raw', 'raw_business') }}

)

SELECT
    id AS business_id,
    create_date AS business_create_date,
    country AS business_country,
    organizational_type,
    type AS business_type,
    subtype AS business_subtype
FROM source
