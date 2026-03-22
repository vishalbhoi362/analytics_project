{{ config(
    materialized='incremental',
    unique_key='response_id'
) }}

WITH source AS (
    SELECT * 
    FROM {{ ref('stg_fact_campaign_response') }}
),

transformed AS (
    SELECT
    
        {{ dbt_utils.generate_surrogate_key([
            'response_id',
            'loaded_at'
        ]) }} AS response_sk,
        response_id,
        campaign_id,
        hcp_id,
        channel,
        response_type,
        response_date,
        loaded_at
    FROM source
    WHERE response_id IS NOT NULL
)

SELECT * 
FROM transformed

{% if is_incremental() %}
WHERE loaded_at > (
    SELECT COALESCE(MAX(loaded_at), '1900-01-01')
    FROM {{ this }}
)
{% endif %}

