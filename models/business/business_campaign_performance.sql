{{ config(materialized='view') }}

SELECT
    f.sales_id,
    f.sale_date,
    f.units_sold,
    f.revenue,

    c.campaign_name,
    c.channel_code,

    d.drug_name,
    h.hcp_name,
    r.rep_name

FROM {{ ref('fact_sales') }} f

LEFT JOIN {{ ref('dim_campaign') }} c
    ON f.campaign_sk = c.campaign_sk

LEFT JOIN {{ ref('dim_drug') }} d
    ON f.drug_sk = d.drug_sk

LEFT JOIN {{ ref('dim_hcp') }} h
    ON f.hcp_sk = h.hcp_sk

LEFT JOIN {{ ref('dim_rep') }} r
    ON f.rep_sk = r.rep_sk

WHERE c.is_current = TRUE