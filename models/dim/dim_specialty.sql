{{ config(
    materialized='table'
) }}

WITH snapshot_data AS (

    SELECT *
    FROM {{ ref('specialty_snapshot') }}

),

final AS (

    SELECT
        specialty_code_sk,
        specialty_code,
        specialty_name,
        loaded_at,

        -- SCD Type 2 columns
        dbt_valid_from,
        dbt_valid_to,

        -- Current flag
        CASE 
            WHEN dbt_valid_to IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current

    FROM snapshot_data

)

SELECT * 
FROM final