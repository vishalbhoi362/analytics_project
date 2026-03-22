WITH source AS (

SELECT *
FROM {{ source('healthcare_raw','stg_drug') }}

),

renamed AS (

SELECT
    drug_id::NUMBER                AS drug_id,
    drug_name                      AS drug_name,
    therapeutic_area_code::NUMBER  AS therapeutic_area_code,
    product_line_code::NUMBER      AS product_line_code,
    loaded_at::TIMESTAMP           AS loaded_at

FROM source
WHERE drug_id IS NOT NULL

QUALIFY ROW_NUMBER() OVER(
    PARTITION BY drug_id
    ORDER BY loaded_at DESC
) = 1

)

SELECT * FROM renamed