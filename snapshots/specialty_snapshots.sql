{% snapshot specialty_snapshot %}

{{
    config(
        unique_key='specialty_code',
        strategy='timestamp',
        updated_at='loaded_at'
    )
}}

SELECT * 
FROM {{ ref('int_specialty') }}

{% endsnapshot %}
