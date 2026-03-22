{{ config(
    materialized='incremental',
    unique_key='channel_code',
    on_schema_change='sync_all_columns'
) }}

WITH source AS (
    SELECT * 
    FROM {{ ref('stg_channel_codes') }}
),

transformed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'channel_code',
            'loaded_at'
        ]) }} AS channel_sk,
        channel_code,
        channel_name,
        loaded_at
    FROM source
    WHERE channel_code IS NOT NULL
)

SELECT * 
FROM transformed

{% if is_incremental() %}
WHERE loaded_at > (
    SELECT COALESCE(MAX(loaded_at), '1900-01-01')
    FROM {{ this }}
)
{% endif %}

