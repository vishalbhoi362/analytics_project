WITH source AS (

SELECT *
FROM {{ source('healthcare_raw','stg_channel_codes') }}

),

renamed AS (

SELECT
    channel_code::NUMBER      AS channel_code,
    channel_name              AS channel_name,
    loaded_at::TIMESTAMP      AS loaded_at

FROM source
WHERE channel_code IS NOT NULL

QUALIFY ROW_NUMBER() OVER(
    PARTITION BY channel_code
    ORDER BY loaded_at DESC
) = 1

)

SELECT * FROM renamed