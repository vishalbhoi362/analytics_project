{% snapshot campaign_snapshot %}

{{
    config(
        unique_key='campaign_id',
        strategy='timestamp',
        updated_at='loaded_at'
    )
}}

SELECT *
FROM {{ ref('int_campaign') }}

{% endsnapshot %}