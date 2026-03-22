{{ config(
    materialized='table'
) }}

WITH snapshot_data AS (

    SELECT *
    FROM {{ ref('hospital_snapshot') }}

),

final AS (

    SELECT
       hospital_sk, 
        hospital_id,
        hospital_name,
        region_code,
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
