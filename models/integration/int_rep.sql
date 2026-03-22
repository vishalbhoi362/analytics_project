{{ config(
    materialized='incremental',
    unique_key='rep_id'
) }}

WITH source AS (
    SELECT *
    FROM {{ ref( 'stg_rep') }}
),

renamed AS (
    SELECT
    
        {{ dbt_utils.generate_surrogate_key([
            'rep_id',
            'loaded_at'
        ]) }} AS rep_sk,
        rep_id,
        rep_name,
        region_code,
        loaded_at
    FROM source
    WHERE rep_id IS NOT NULL

)

SELECT * 
FROM renamed

{% if is_incremental() %}
WHERE loaded_at >= (
    SELECT COALESCE(MAX(loaded_at), '1900-01-01')
    FROM {{ this }}
)
{% endif %}