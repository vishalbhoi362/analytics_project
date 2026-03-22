{{ config(
    materialized='view'
) }}

WITH source AS (

    SELECT *
    FROM {{ source('healthcare_raw','fact_rep_performance') }}

),

renamed AS (

    SELECT
        performance_id::NUMBER     AS performance_id,
        rep_id::NUMBER             AS sales_rep_id,
        campaign_id::NUMBER        AS campaign_id,
        TO_DATE(activity_date)     AS activity_date,
        meetings_count::NUMBER     AS meetings_count,
        pipeline_value::NUMBER     AS pipeline_value,
        loaded_at::TIMESTAMP       AS loaded_at

    FROM source
    WHERE performance_id IS NOT NULL

    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY performance_id
        ORDER BY loaded_at DESC
    ) = 1

)

SELECT * FROM renamed