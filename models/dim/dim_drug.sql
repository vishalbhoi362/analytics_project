{{ config(
    materialized='table'
) }}

WITH snapshot_data AS (

    SELECT *
    FROM {{ ref('drug_snapshot') }}

),

final AS (

    SELECT
       drug_sk, 
        drug_id,
        drug_name,
        therapeutic_area_code,
        product_line_code,
        loaded_at,
        dbt_valid_from,
        dbt_valid_to,
        CASE 
            WHEN dbt_valid_to IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current

    FROM snapshot_data

)

SELECT * FROM final

