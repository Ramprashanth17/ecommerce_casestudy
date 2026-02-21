SELECT
"order_id" :: TEXT AS order_id,
"customer_id" :: TEXT AS customer_id,
"order_status" :: TEXT AS order_status,
"order_purchase_timestamp" :: TIMESTAMP AS order_purchase_timestamp ,
"order_approved_at" :: TIMESTAMP AS order_approved_at,
"order_delivered_carrier_date" :: TIMESTAMP AS order_delivered_carrier_date,
"order_delivered_customer_date" :: TIMESTAMP AS order_delivered_customer_date,
"order_estimated_delivery_date" :: TIMESTAMP AS order_estimated_delivery_date
FROM {{ source('RAW', 'ORDERS') }}
