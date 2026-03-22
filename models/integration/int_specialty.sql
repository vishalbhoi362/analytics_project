{{ config(
    materialized='incremental',
    unique_key='specialty_code',
    on_schema_change='sync_all_columns'
) }}

WITH source AS (
    SELECT *
    FROM {{ ref('stg_specialty') }}
),

renamed AS (
    
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'specialty_code',
            'loaded_at'
        ]) }} AS specialty_code_sk,
        specialty_code,
        specialty_name,
        loaded_at
    FROM source
    WHERE specialty_code IS NOT NULL

)

SELECT * 
FROM renamed

{% if is_incremental() %}
WHERE loaded_at >= (
    SELECT COALESCE(MAX(loaded_at), '1900-01-01')
    FROM {{ this }}
)
{% endif %}