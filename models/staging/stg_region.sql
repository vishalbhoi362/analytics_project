WITH source AS (

SELECT *
FROM {{ source('healthcare_raw','stg_region') }}

),

renamed AS (

SELECT
    region_code::NUMBER     AS region_code,
    region_name             AS region_name,
    loaded_at::TIMESTAMP    AS loaded_at

FROM source
WHERE region_code IS NOT NULL

QUALIFY ROW_NUMBER() OVER(
    PARTITION BY region_code
    ORDER BY loaded_at DESC
) = 1

)

SELECT * FROM renamed