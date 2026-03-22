{{ config(
    materialized='incremental',
    unique_key='performance_id'
) }}

WITH source AS (
    SELECT * 
    FROM {{ ref('stg_fact_rep_performance') }}
),

transformed AS (
    SELECT
    
        {{ dbt_utils.generate_surrogate_key([
            'performance_id',
            'loaded_at'
        ]) }} AS performance_sk,
        performance_id,
        sales_rep_id,
        campaign_id,
        activity_date,
        meetings_count,
        pipeline_value,
        loaded_at
    FROM source
    WHERE performance_id IS NOT NULL
)

SELECT * 
FROM transformed

{% if is_incremental() %}
WHERE loaded_at > (
    SELECT COALESCE(MAX(loaded_at), '1990-01-01')
    FROM {{ this }}
)
{% endif %}