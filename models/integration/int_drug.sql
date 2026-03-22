{{ config(
    materialized='incremental',
    unique_key='drug_id'
) }}

WITH source AS (
    SELECT * 
    FROM {{ ref('stg_drug') }}
),

transformed AS (
    SELECT
    
        {{ dbt_utils.generate_surrogate_key([
            'drug_id',
            'loaded_at'
        ]) }} AS drug_sk, 
        drug_id,
        drug_name,
        therapeutic_area_code,
        product_line_code,
        loaded_at
    FROM source 
    WHERE drug_id IS NOT NULL
)

SELECT * 
FROM transformed

{% if is_incremental() %}
WHERE loaded_at > (
    SELECT COALESCE(MAX(loaded_at), '1900-01-01')
    FROM {{ this }}
)
{% endif %}