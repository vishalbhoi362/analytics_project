WITH source AS (

SELECT *
FROM {{ source('healthcare_raw','stg_campaign_phone') }}

),

renamed AS (

SELECT
    call_id::NUMBER          AS call_id,
    campaign_id::NUMBER      AS campaign_id,
    hcp_id::NUMBER           AS hcp_id,
    TO_DATE(call_date)       AS call_date,
    status                   AS call_status,
    duration_sec::NUMBER     AS call_duration_seconds,
    drug_id::NUMBER          AS drug_id,
    rep_id::NUMBER           AS sales_rep_id,
    loaded_at::TIMESTAMP     AS loaded_at

FROM source
WHERE call_id IS NOT NULL

QUALIFY ROW_NUMBER() OVER(
    PARTITION BY call_id
    ORDER BY loaded_at DESC
) = 1

)

SELECT * FROM renamed