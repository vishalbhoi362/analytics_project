WITH source AS (

SELECT *
FROM {{ source('healthcare_raw','stg_product_line') }}

),

renamed AS (

SELECT
    product_line_code::NUMBER   AS product_line_code,
    product_line_name           AS product_line_name,
    loaded_at::TIMESTAMP        AS loaded_at

FROM source
WHERE product_line_code IS NOT NULL

QUALIFY ROW_NUMBER() OVER(
    PARTITION BY product_line_code
    ORDER BY loaded_at DESC
) = 1

)

SELECT * FROM renamed