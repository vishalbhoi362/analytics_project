WITH source AS (

SELECT *
FROM {{ source('healthcare_raw','stg_therapeutic_area') }}

),

renamed AS (

SELECT
    therapeutic_area_code::NUMBER   AS therapeutic_area_code,
    therapeutic_area_name           AS therapeutic_area_name,
    loaded_at::TIMESTAMP            AS loaded_at

FROM source
WHERE therapeutic_area_code IS NOT NULL

QUALIFY ROW_NUMBER() OVER(
    PARTITION BY therapeutic_area_code
    ORDER BY loaded_at DESC
) = 1

)

SELECT * FROM renamed