{{ config(
    materialized='incremental',
    unique_key='sales_id'
) }}

WITH source AS (
    SELECT * 
    FROM {{ ref('int_fact_sales') }}
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
            'sales_id',
            's.loaded_at'
        ]) }} AS sales_sk,

        -- Foreign Keys (VERY IMPORTANT)
        h.hcp_sk,
        d.drug_sk,
        c.campaign_sk,
        r.rep_sk,

        -- Natural Keys
        s.sales_id,
        s.hcp_id,
        s.drug_id,
        s.campaign_id,
        s.sales_rep_id,

        -- Measures
        s.sale_date,
        s.units_sold,
        s.revenue,
        s.loaded_at

    FROM source s

    -- SCD2 Join: HCP
    LEFT JOIN hcp_dim h
        ON s.hcp_id = h.hcp_id
       /* AND s.sale_date BETWEEN h.dbt_valid_from 
                           AND COALESCE(h.dbt_valid_to, '9999-12-31')
*/
    -- SCD2 Join: Drug
    LEFT JOIN drug_dim d
        ON s.drug_id = d.drug_id
   /*     AND s.sale_date BETWEEN d.dbt_valid_from 
                            AND COALESCE(d.dbt_valid_to, '9999-12-31')
*/
    -- SCD2 Join: Campaign
    LEFT JOIN campaign_dim c
        ON s.campaign_id = c.campaign_id
  /*      AND s.sale_date BETWEEN c.dbt_valid_from 
                            AND COALESCE(c.dbt_valid_to, '9999-12-31')
*/
    -- SCD2 Join: Sales Rep
    LEFT JOIN rep_dim r
        ON s.sales_rep_id = r.rep_id
/*        AND s.sale_date BETWEEN r.dbt_valid_from 
                           AND COALESCE(r.dbt_valid_to, '9999-12-31')
*/
)

SELECT * 
FROM final

{% if is_incremental() %}
WHERE loaded_at >= (
    SELECT COALESCE(MAX(loaded_at), '1900-01-01')
    FROM {{ this }}
)
{% endif %}