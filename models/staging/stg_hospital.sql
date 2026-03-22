WITH source AS (

SELECT *
FROM {{ source('healthcare_raw','stg_hospital') }}

),

renamed AS (

SELECT
    hospital_id::NUMBER      AS hospital_id,
    hospital_name            AS hospital_name,
    region_code::NUMBER      AS region_code,
    loaded_at::TIMESTAMP     AS loaded_at

FROM source
WHERE hospital_id IS NOT NULL

QUALIFY ROW_NUMBER() OVER(
    PARTITION BY hospital_id
    ORDER BY loaded_at DESC
) = 1

)

SELECT * FROM renamed