{{ config(
    materialized='view'
) }}

WITH source AS (

    SELECT *
    FROM {{ source('healthcare_raw','fact_campaign_response') }}

),

renamed AS (

    SELECT
        response_id::NUMBER        AS response_id,
        campaign_id::NUMBER        AS campaign_id,
        hcp_id::NUMBER             AS hcp_id,
        channel                    AS channel,
        response_type              AS response_type,
        TO_DATE(response_date)     AS response_date,
        loaded_at::TIMESTAMP       AS loaded_at

    FROM source
    WHERE response_id IS NOT NULL

    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY response_id
        ORDER BY loaded_at DESC
    ) = 1

)

SELECT * FROM renamed