{{ config(
    materialized='view'
) }}

WITH source AS (

    SELECT *
    FROM {{ source('healthcare_raw','stg_campaign') }}

),

renamed AS (

    SELECT
        campaign_id::NUMBER        AS campaign_id,
        campaign_name              AS campaign_name,
        drug_id::NUMBER            AS drug_id,
        channel_code::NUMBER       AS channel_code,
        TO_DATE(start_date)        AS start_date,
        TO_DATE(end_date)          AS end_date,
        budget::NUMBER             AS campaign_budget,
        rep_id::NUMBER             AS sales_rep_id,
        loaded_at::TIMESTAMP       AS loaded_at

    FROM source
    WHERE campaign_id IS NOT NULL

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY campaign_id
        ORDER BY loaded_at DESC
    ) = 1

)

SELECT *
FROM renamed