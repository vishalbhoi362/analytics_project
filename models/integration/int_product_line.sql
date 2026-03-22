{{ config(
    materialized='incremental',
    unique_key='product_line_code'
) }}

WITH source AS (
    SELECT *
    FROM {{ ref( 'stg_product_line') }}
),

renamed AS (
    SELECT
    
        {{ dbt_utils.generate_surrogate_key([
            'product_line_code',
            'loaded_at'
        ]) }} AS product_line_sk,
        product_line_code,
        product_line_name,
        loaded_at
    FROM source
    WHERE product_line_code IS NOT NULL

)

SELECT * 
FROM renamed

{% if is_incremental() %}
WHERE loaded_at >= (
    SELECT COALESCE(MAX(loaded_at), '1900-01-01')
    FROM {{ this }}
)
{% endif %}