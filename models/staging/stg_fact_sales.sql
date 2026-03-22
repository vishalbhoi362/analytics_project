{{ config(
    materialized='view'
) }}

WITH source AS (

    SELECT *
    FROM {{ source('healthcare_raw','fact_sales') }}

),

renamed AS (

    SELECT
        sales_id::NUMBER           AS sales_id,
        drug_id::NUMBER            AS drug_id,
        hcp_id::NUMBER             AS hcp_id,
        campaign_id::NUMBER        AS campaign_id,
        rep_id::NUMBER             AS sales_rep_id,
        TO_DATE(sale_date)         AS sale_date,
        units_sold::NUMBER         AS units_sold,
        revenue::NUMBER            AS revenue,
        loaded_at::TIMESTAMP       AS loaded_at

    FROM source
    WHERE sales_id IS NOT NULL

    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY sales_id
        ORDER BY loaded_at DESC
    ) = 1

)

SELECT * FROM renamed