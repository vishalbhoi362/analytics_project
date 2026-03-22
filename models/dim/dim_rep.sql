{{ config(
    materialized='table'
) }}

WITH snapshot_data AS (

    SELECT *
    FROM {{ ref('rep_snapshot') }}

),

final AS (

    SELECT
       rep_sk,
        rep_id,
        rep_name,
        region_code,
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