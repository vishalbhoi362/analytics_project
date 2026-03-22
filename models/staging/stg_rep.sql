WITH source AS (

SELECT *
FROM {{ source('healthcare_raw','stg_rep') }}

),

renamed AS (

SELECT
    rep_id::NUMBER        AS rep_id,
    rep_name              AS rep_name,
    region_code::NUMBER   AS region_code,
    loaded_at::TIMESTAMP  AS loaded_at

FROM source
WHERE rep_id IS NOT NULL

QUALIFY ROW_NUMBER() OVER(
    PARTITION BY rep_id
    ORDER BY loaded_at DESC
) = 1

)

SELECT * FROM renamed