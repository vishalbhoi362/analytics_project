{{ config(
    materialized='incremental',
    unique_key='region_code'
) }}

WITH source AS (
    SELECT *
    FROM {{ ref( 'stg_region') }}
),

renamed AS (
    SELECT
    
        {{ dbt_utils.generate_surrogate_key([
            'region_code',
            'loaded_at'
        ]) }} AS region_sk,
        region_code,
        region_name,
        loaded_at
    FROM source
    WHERE region_code IS NOT NULL

)

SELECT * 
FROM renamed

{% if is_incremental() %}
WHERE loaded_at >= (
    SELECT COALESCE(MAX(loaded_at), '1900-01-01')
    FROM {{ this }}
)
{% endif %}