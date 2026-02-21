SELECT 
"order_id" :: TEXT AS order_id,
"order_item_id" :: NUMBER AS order_item_id,
"product_id" :: TEXT AS product_id,
"seller_id" :: TEXT AS seller_id,
"shipping_limit_date" :: TIMESTAMP AS shipping_limit_date,
"price" :: DECIMAL(10, 2) AS price,
"freight_value" :: DECIMAL(10, 2) AS freight_value
FROM {{ source("RAW", "ORDER_ITEMS") }}