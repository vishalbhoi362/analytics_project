{{ config(
    materialized='incremental',
    unique_key='performance_id'
) }}

WITH source AS (
    SELECT * 
    FROM {{ ref('int_fact_rep_performance') }}
),

rep_dim AS (
    SELECT * FROM {{ ref('dim_rep') }}
),

campaign_dim AS (
    SELECT * FROM {{ ref('dim_campaign') }}
),

final AS (

    SELECT
        -- Fact Surrogate Key
        {{ dbt_utils.generate_surrogate_key([
            'performance_id',
            's.loaded_at'
        ]) }} AS performance_sk,

        -- Foreign Keys (VERY IMPORTANT)
        r.rep_sk,
        c.campaign_sk,

        -- Natural Keys
        s.performance_id,
        s.sales_rep_id,
        s.campaign_id,

        -- Measures
        s.activity_date,
        s.meetings_count,
        s.pipeline_value,
        s.loaded_at

    FROM source s

    -- SCD2 Join: Rep
    LEFT JOIN rep_dim r
        ON s.sales_rep_id = r.rep_id
        AND s.activity_date BETWEEN r.dbt_valid_from 
                               AND COALESCE(r.dbt_valid_to, '9999-12-31')

    -- SCD2 Join: Campaign
    LEFT JOIN campaign_dim c
        ON s.campaign_id = c.campaign_id
        AND s.activity_date BETWEEN c.dbt_valid_from 
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