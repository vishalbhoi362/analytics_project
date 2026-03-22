WITH source AS (

SELECT *
FROM {{ source('healthcare_raw','stg_specialty') }}

),

renamed AS (

SELECT
    specialty_code::NUMBER   AS specialty_code,
    specialty_name           AS specialty_name,
    loaded_at::TIMESTAMP     AS loaded_at

FROM source
WHERE specialty_code IS NOT NULL

QUALIFY ROW_NUMBER() OVER(
    PARTITION BY specialty_code
    ORDER BY loaded_at DESC
) = 1

)

SELECT * FROM renamed