{% snapshot channel_code_snapshot %}

{{
    config(
        unique_key='channel_code',
        strategy='timestamp',
        updated_at='loaded_at'
    )
}}

SELECT * 
FROM {{ ref('int_channel_code') }}

{% endsnapshot %}