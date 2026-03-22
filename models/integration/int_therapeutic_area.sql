{{ config(
    materialized='incremental',
    unique_key='therapeutic_area_code',
    on_schema_change='sync_all_columns'
) }}

WITH source AS (
    SELECT *
    FROM {{ ref('stg_therapeutic_area') }}
),

renamed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'therapeutic_area_code',
            'loaded_at'
        ]) }} AS therapeutic_area_code_sk,
        therapeutic_area_code,
        therapeutic_area_name,
        loaded_at
    FROM source
    WHERE therapeutic_area_code IS NOT NULL

)

SELECT * 
FROM renamed

{% if is_incremental() %}
WHERE loaded_at >= (
    SELECT COALESCE(MAX(loaded_at), '1900-01-01')
    FROM {{ this }}
)
{% endif %}