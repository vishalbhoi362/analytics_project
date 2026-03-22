{{ config(
    materialized='incremental',
    unique_key='prescription_id'
) }}

WITH source AS (
    SELECT * 
    FROM {{ ref('int_fact_prescription') }}
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

final AS (

    SELECT
        -- Fact Surrogate Key
        {{ dbt_utils.generate_surrogate_key([
            'prescription_id',
            's.loaded_at'
        ]) }} AS prescription_sk,

        -- Foreign Keys (VERY IMPORTANT)
        h.hcp_sk,
        d.drug_sk,
        c.campaign_sk,

        -- Natural Keys (optional)
        s.prescription_id,
        s.hcp_id,
        s.drug_id,
        s.campaign_id,

        -- Measures
        s.prescription_date,
        s.quantity,
        s.loaded_at

    FROM source s

    -- SCD2 Join: HCP
    LEFT JOIN hcp_dim h
        ON s.hcp_id = h.hcp_id
        AND s.prescription_date BETWEEN h.dbt_valid_from 
                                   AND COALESCE(h.dbt_valid_to, '9999-12-31')

    -- SCD2 Join: Drug
    LEFT JOIN drug_dim d
        ON s.drug_id = d.drug_id
        AND s.prescription_date BETWEEN d.dbt_valid_from 
                                    AND COALESCE(d.dbt_valid_to, '9999-12-31')

    -- SCD2 Join: Campaign
    LEFT JOIN campaign_dim c
        ON s.campaign_id = c.campaign_id
        AND s.prescription_date BETWEEN c.dbt_valid_from 
                                    AND COALESCE(c.dbt_valid_to, '9999-12-31')

)

SELECT * 
FROM final

{% if is_incremental() %}
WHERE loaded_at >= (
    SELECT COALESCE(MAX(loaded_at), '1900-01-01')
    FROM {{ this }}
)
{% endif %}