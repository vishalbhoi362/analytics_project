{% snapshot therapeutic_area_snapshot %}

{{
    config(
        unique_key='therapeutic_area_code',
        strategy='timestamp',
        updated_at='loaded_at'
    )
}}

SELECT * 
FROM {{ ref('int_therapeutic_area') }}

{% endsnapshot %}