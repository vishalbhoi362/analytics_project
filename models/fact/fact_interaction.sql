{{ config(
    materialized='incremental',
    unique_key='interaction_id'
) }}

WITH source AS (
    SELECT * 
    FROM {{ ref('int_campaign_interactions') }}
),

hcp_dim AS (
    SELECT * FROM {{ ref('dim_hcp') }}
),

drug_dim AS (
    SELECT * FROM {{ ref('dim_drug') }}
),

campaign_dim AS (
    SELECT * FROM {{ ref('dim_campaign') }}
),

rep_dim AS (
    SELECT * FROM {{ ref('dim_rep') }}
),

final AS (

    SELECT
        -- Fact Surrogate Key
        {{ dbt_utils.generate_surrogate_key([
            'interaction_id',
            'interaction_channel',
            's.loaded_at'
        ]) }} AS interaction_sk,

        -- Foreign Keys
        h.hcp_sk,
        d.drug_sk,
        c.campaign_sk,
        r.rep_sk,

        -- Natural Keys
        s.interaction_id,
        s.campaign_id,
        s.hcp_id,
        s.drug_id,
        s.sales_rep_id,

        -- Interaction Details
        s.interaction_channel,
        s.interaction_date,

        -- Email fields
        s.email_status,
        s.email_subject,

        -- Call fields
        s.call_status,
        s.call_duration_seconds,

        -- Event fields
        s.event_status,
        s.event_location,
        s.event_type,

        s.loaded_at

    FROM source s

    -- SCD2 Join: HCP
    LEFT JOIN hcp_dim h
        ON s.hcp_id = h.hcp_id
        AND s.interaction_date BETWEEN h.dbt_valid_from 
                                  AND COALESCE(h.dbt_valid_to, '9999-12-31')

    -- SCD2 Join: Drug
    LEFT JOIN drug_dim d
        ON s.drug_id = d.drug_id
        AND s.interaction_date BETWEEN d.dbt_valid_from 
                                  AND COALESCE(d.dbt_valid_to, '9999-12-31')

    -- SCD2 Join: Campaign
    LEFT JOIN campaign_dim c
        ON s.campaign_id = c.campaign_id
        AND s.interaction_date BETWEEN c.dbt_valid_from 
                                  AND COALESCE(c.dbt_valid_to, '9999-12-31')

    -- SCD2 Join: Sales Rep (only for PHONE/EVENT)
    LEFT JOIN rep_dim r
        ON s.sales_rep_id = r.rep_id
        AND s.interaction_date BETWEEN r.dbt_valid_from 
                                  AND COALESCE(r.dbt_valid_to, '9999-12-31')

)

SELECT *
FROM final

{% if is_incremental() %}
WHERE loaded_at >= (
    SELECT COALESCE(MAX(loaded_at), '1900-01-01')
    FROM {{ this }}
)
{% endif %}