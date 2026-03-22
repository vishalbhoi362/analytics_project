{% snapshot drug_snapshot %}

{{
    config(
        unique_key='drug_id',
        strategy='timestamp',
        updated_at='loaded_at'
    )
}}

SELECT * 
FROM {{ ref('int_drug') }}

{% endsnapshot %}