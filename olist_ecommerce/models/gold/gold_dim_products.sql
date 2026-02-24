SELECT 
{{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk,
product_id,
product_name_english,
product_width_cm,
product_weight_g,
product_length_cm,
product_height_cm,
product_category_name
FROM {{ ref('silver_products') }}