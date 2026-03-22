{{ config(
    materialized='incremental',
    unique_key='prescription_id'
) }}

WITH source AS (
    SELECT * 
    FROM {{ ref('stg_fact_prescription') }}
),

transformed AS (
    SELECT
    
        {{ dbt_utils.generate_surrogate_key([
            'prescription_id',
            'loaded_at'
        ]) }} AS prescription_sk,
        prescription_id,
        hcp_id,
        drug_id,
        campaign_id,
        prescription_date,
        quantity,
        loaded_at
    FROM source 
    WHERE prescription_id IS NOT NULL
)

SELECT * 
FROM transformed

{% if is_incremental() %}
WHERE loaded_at > (
    SELECT COALESCE(MAX(loaded_at), '1900-01-01')
    FROM {{ this }}
)
{% endif %}

