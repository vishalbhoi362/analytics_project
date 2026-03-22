{% snapshot region_snapshot %}

{{
    config(
        unique_key='region_code',
        strategy='timestamp',
        updated_at='loaded_at'
    )
}}

SELECT * 
FROM {{ ref('int_region')}}

{% endsnapshot %}
