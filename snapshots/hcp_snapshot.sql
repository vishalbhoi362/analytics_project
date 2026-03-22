{% snapshot hcp_snapshot %}

{{
    config(
        unique_key='hcp_id',
        strategy='timestamp',
        updated_at='loaded_at'
    )
}}

SELECT *
FROM {{ ref('int_hcp') }}

{% endsnapshot %}