{% snapshot hospital_snapshot %}

{{
    config(
        unique_key='hospital_id',
        strategy='timestamp',
        updated_at='loaded_at'
    )
}}

SELECT * 
FROM {{ ref('int_hospital') }}

{% endsnapshot %}