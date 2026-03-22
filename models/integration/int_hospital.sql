{{ config(
    materialized='incremental',
    unique_key='hospital_id'
) }}

WITH source AS (
    SELECT * 
    FROM {{ ref('stg_hospital') }}
),

transformed AS (
    SELECT
    
        {{ dbt_utils.generate_surrogate_key([
            'hospital_id',
            'loaded_at'
        ]) }} AS hospital_sk, 
        hospital_id,
        hospital_name,
        region_code,
        loaded_at,
       
    FROM source 
    WHERE hospital_id IS NOT NULL
)

SELECT * 
FROM transformed

{% if is_incremental() %}
WHERE loaded_at > (
    SELECT COALESCE(MAX(loaded_at), '1900-01-01')
    FROM {{ this }}
)
{% endif %}