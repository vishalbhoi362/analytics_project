WITH source AS (

SELECT *
FROM {{ source('healthcare_raw','stg_campaign_email') }}

),

renamed AS (

SELECT
    email_id::NUMBER          AS email_id,
    campaign_id::NUMBER       AS campaign_id,
    hcp_id::NUMBER            AS hcp_id,
    TO_DATE(send_date)        AS send_date,
    status                    AS email_status,
    drug_id::NUMBER           AS drug_id,
    subject_line              AS email_subject,
    loaded_at::TIMESTAMP      AS loaded_at

FROM source
WHERE email_id IS NOT NULL

QUALIFY ROW_NUMBER() OVER(
    PARTITION BY email_id
    ORDER BY loaded_at DESC
) = 1

)

SELECT * FROM renamed