{{ config(
    materialized='incremental',
    unique_key='interaction_id',
    on_schema_change='sync_all_columns'
) }}

WITH email AS (

SELECT
     HASH(email_id, 'EMAIL') AS interaction_id,
    campaign_id,
    hcp_id,
    drug_id,
    send_date                    AS interaction_date,

    email_status,
    email_subject,

    NULL::STRING                 AS call_status,
    NULL::NUMBER                 AS call_duration_seconds,

    NULL::STRING                 AS event_status,
    NULL::STRING                 AS event_location,
    NULL::STRING                 AS event_type,

    NULL::NUMBER                 AS sales_rep_id,

    'EMAIL'                      AS interaction_channel,
    loaded_at

FROM {{ ref('stg_campaign_email') }}

),

phone AS (

SELECT
    HASH(call_id, 'PHONE') AS   interaction_id,
    campaign_id,
    hcp_id,
    drug_id,
    call_date                    AS interaction_date,

    NULL::STRING                 AS email_status,
    NULL::STRING                 AS email_subject,

    call_status,
    call_duration_seconds,

    NULL::STRING                 AS event_status,
    NULL::STRING                 AS event_location,
    NULL::STRING                 AS event_type,

    sales_rep_id,

    'PHONE'                      AS interaction_channel,
    loaded_at

FROM {{ ref('stg_campaign_phone') }}

),

event AS (

SELECT
    HASH(event_id, 'EVENT') AS    interaction_id,
    campaign_id,
    hcp_id,
    drug_id,
    event_date                   AS interaction_date,

    NULL::STRING                 AS email_status,
    NULL::STRING                 AS email_subject,

    NULL::STRING                 AS call_status,
    NULL::NUMBER                 AS call_duration_seconds,

    event_status,
    event_location,
    event_type,

    sales_rep_id,

    'EVENT'                      AS interaction_channel,
    loaded_at

FROM {{ ref('stg_campaign_event') }}

),

combined AS (

SELECT * FROM email
UNION ALL
SELECT * FROM phone
UNION ALL
SELECT * FROM event

),

transformed AS (

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'interaction_id', 
        'interaction_channel',
        'loaded_at'
    ]) }} AS interaction_sk,
    *
FROM combined

)

SELECT *
FROM transformed

{% if is_incremental() %}

WHERE loaded_at >= (
    SELECT COALESCE(MAX(loaded_at), '1900-01-01')
    FROM {{ this }}
)

{% endif %}