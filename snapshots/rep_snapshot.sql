{% snapshot rep_snapshot %}

{{
    config(
        unique_key='rep_id',
        strategy='timestamp',
        updated_at='loaded_at'
    )
}}

SELECT * 
FROM {{ ref('int_rep') }}

{% endsnapshot %}
