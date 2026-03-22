{{ config(
    materialized='table'
) }}

WITH snapshot_data AS (

    SELECT *
    FROM {{ ref('channel_code_snapshot') }}

),

final AS (

    SELECT
       channel_sk,
        channel_code,
        channel_name,
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