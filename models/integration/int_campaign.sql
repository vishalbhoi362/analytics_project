{{ config(
    materialized='incremental',
    unique_key='campaign_id',
    on_schema_change='sync_all_columns'
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('stg_campaign') }}

),

transformed AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'campaign_id',
            'loaded_at'
        ]) }} AS campaign_sk,


        campaign_id,
        campaign_name,
        drug_id,
        channel_code,
        start_date,
        end_date,
        campaign_budget,
        sales_rep_id,
        loaded_at

    FROM source
    WHERE campaign_id IS NOT NULL

)

SELECT *
FROM transformed

{% if is_incremental() %}

WHERE loaded_at >= (
    SELECT COALESCE(MAX(loaded_at), '1900-01-01')
    FROM {{ this }}
)

{% endif %}