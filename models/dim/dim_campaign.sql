{{ config(
    materialized='table'
) }}

WITH snapshot_data AS (

    SELECT *
    FROM {{ ref('campaign_snapshot') }}

),

final AS (

    SELECT
        campaign_sk,
        campaign_id,
        campaign_name,
        drug_id,
        channel_code,
        start_date,
        end_date,
        campaign_budget,
        sales_rep_id,
        dbt_valid_from,
        dbt_valid_to,
        CASE 
            WHEN dbt_valid_to IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current

    FROM snapshot_data

)

SELECT * FROM final