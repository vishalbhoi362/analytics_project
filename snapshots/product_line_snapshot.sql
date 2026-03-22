{% snapshot product_line_snapshot %}

{{
    config(
        unique_key='product_line_code',
        strategy='timestamp',
        updated_at='loaded_at'
    )
}}

SELECT * 
FROM {{ ref('int_product_line') }}

{% endsnapshot %}