SELECT 
"product_category_name" :: TEXT AS product_category_name,
"product_description_lenght" :: NUMBER AS product_description_length,
"product_height_cm" :: NUMBER AS product_height_cm,
"product_id" :: TEXT AS product_id,
"product_length_cm" :: NUMBER AS product_length_cm,
"product_name_lenght" :: NUMBER AS product_name_length,
"product_photos_qty" :: NUMBER AS product_photos_qty,
"product_weight_g" :: NUMBER AS product_weight_g,
"product_width_cm" :: NUMBER AS product_width_cm
FROM {{ source('RAW', 'PRODUCTS') }}