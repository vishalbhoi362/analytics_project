{{ config(
    materialized='incremental',
    unique_key='sales_id'
) }}

WITH source AS (
    SELECT * 
    FROM {{ ref('stg_fact_sales') }}
),

transformed AS (
    SELECT
    
        {{ dbt_utils.generate_surrogate_key([
            'sales_id',
            'loaded_at'
        ]) }} AS sales_sk, 
        sales_id,
        drug_id,
        hcp_id,
        campaign_id,
        sales_rep_id,
        sale_date,
        units_sold,
        revenue,
        loaded_at
    FROM source 
    WHERE sales_id IS NOT NULL
)

SELECT * 
FROM transformed

{% if is_incremental() %}
WHERE loaded_at > (
    SELECT COALESCE(MAX(loaded_at), '1900-01-01')
    FROM {{ this }}
)
{% endif %}

