{{ config(
    materialized='incremental',
    unique_key='response_id'
) }}

WITH source AS (
    SELECT * 
    FROM {{ ref('int_fact_campaign_response') }}
),

-- Join with Campaign Dimension
campaign_dim AS (
    SELECT *
    FROM {{ ref('dim_campaign') }}
),

-- Join with HCP Dimension
hcp_dim AS (
    SELECT *
    FROM {{ ref('dim_hcp') }}
),

final AS (

    SELECT
        -- Fact Surrogate Key
        {{ dbt_utils.generate_surrogate_key([
            'response_id',
            's.loaded_at'
        ]) }} AS response_sk,

        -- Foreign Keys (VERY IMPORTANT)
        c.campaign_sk,
        h.hcp_sk,
        -- Natural Keys (optional but useful)
        s.response_id,
        s.campaign_id,
        s.hcp_id,

        -- Measures / attributes
        s.channel,
        s.response_type,
        s.response_date,
        s.loaded_at

    FROM source s

    LEFT JOIN campaign_dim c
        ON s.campaign_id = c.campaign_id
        AND s.response_date BETWEEN c.dbt_valid_from 
                                AND COALESCE(c.dbt_valid_to, '9999-12-31')

    LEFT JOIN hcp_dim h
        ON s.hcp_id = h.hcp_id
        AND s.response_date BETWEEN h.dbt_valid_from 
                                AND COALESCE(h.dbt_valid_to, '9999-12-31')

)

SELECT * 
FROM final

{% if is_incremental() %}
WHERE loaded_at >= (
    SELECT COALESCE(MAX(loaded_at), '1900-01-01')
    FROM {{ this }}
)
{% endif %}