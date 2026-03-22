{{ config(
    materialized='table'
) }}

WITH snapshot_data AS (

    SELECT *
    FROM {{ ref('hcp_snapshot') }}

),

final AS (

    SELECT
       hcp_sk,
        hcp_id,
        hcp_name,
        specialty_code,
        hospital_id,
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
