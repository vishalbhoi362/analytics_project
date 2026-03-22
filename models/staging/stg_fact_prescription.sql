{{ config(
    materialized='view'
) }}

WITH source AS (

    SELECT *
    FROM {{ source('healthcare_raw','fact_prescription') }}

),

renamed AS (

    SELECT
        prescription_id::NUMBER      AS prescription_id,
        hcp_id::NUMBER               AS hcp_id,
        drug_id::NUMBER              AS drug_id,
        campaign_id::NUMBER          AS campaign_id,
        TO_DATE(prescription_date)   AS prescription_date,
        quantity::NUMBER             AS quantity,
        loaded_at::TIMESTAMP         AS loaded_at

    FROM source
    WHERE prescription_id IS NOT NULL

    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY prescription_id
        ORDER BY loaded_at DESC
    ) = 1

)

SELECT * FROM renamed