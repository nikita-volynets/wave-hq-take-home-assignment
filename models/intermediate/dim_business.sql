WITH final AS (
    SELECT *
    FROM {{ ref('stg_business') }}
)

SELECT
    business_id,
    business_create_date,
    business_country,
    organizational_type,
    business_type,
    business_subtype
FROM final
