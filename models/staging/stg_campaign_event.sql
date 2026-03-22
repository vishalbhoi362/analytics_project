WITH source AS (

SELECT *
FROM {{ source('healthcare_raw','stg_campaign_event') }}

),

renamed AS (

SELECT
    event_id::NUMBER          AS event_id,
    campaign_id::NUMBER       AS campaign_id,
    hcp_id::NUMBER            AS hcp_id,
    TO_DATE(event_date)       AS event_date,
    status                    AS event_status,
    drug_id::NUMBER           AS drug_id,
    event_location            AS event_location,
    rep_id::NUMBER            AS sales_rep_id,
    event_type                AS event_type,
    loaded_at::TIMESTAMP      AS loaded_at

FROM source
WHERE event_id IS NOT NULL

QUALIFY ROW_NUMBER() OVER(
    PARTITION BY event_id
    ORDER BY loaded_at DESC
) = 1

)

SELECT * FROM renamed