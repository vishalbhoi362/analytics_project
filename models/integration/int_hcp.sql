{{ config(
    materialized='incremental',
    unique_key='hcp_id',
    on_schema_change='sync_all_columns'
) }}

WITH source AS (
    SELECT * 
    FROM {{ ref('stg_hcp') }}
),

transformed AS (
    SELECT 
    
        {{ dbt_utils.generate_surrogate_key([
            'hcp_id',
            'loaded_at'
            ]) }} AS hcp_sk,
            hcp_id,
            hcp_name,
            specialty_code,
            hospital_id,
            region_code,
        loaded_at
    FROM source 
    WHERE hcp_id IS NOT NULL
)

SELECT * 
FROM transformed

{% if is_incremental() %}
WHERE loaded_at > (
    SELECT COALESCE(MAX(loaded_at), '1900-01-01')
    FROM {{ this }}
)
{% endif %}