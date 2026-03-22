WITH source AS (

SELECT *
FROM {{ source('healthcare_raw','stg_hcp') }}

),

renamed AS (

SELECT
    hcp_id::NUMBER         AS hcp_id,
    hcp_name               AS hcp_name,
    specialty_code::NUMBER AS specialty_code,
    hospital_id::NUMBER    AS hospital_id,
    region_code::NUMBER    AS region_code,
    loaded_at::TIMESTAMP   AS loaded_at

FROM source
WHERE hcp_id IS NOT NULL

QUALIFY ROW_NUMBER() OVER(
    PARTITION BY hcp_id
    ORDER BY loaded_at DESC
) = 1

)

SELECT * FROM renamed