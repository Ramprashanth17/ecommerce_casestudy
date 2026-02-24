-- depends_on: {{ ref('bronze_order_items') }}
-- depends_on: {{ ref('bronze_orders') }}

SELECT 
bo.order_id,
bo.customer_id, 
bo.order_status, 
bo.order_purchase_timestamp,
bo.order_approved_at,
bo.order_delivered_carrier_date,
bo.order_delivered_customer_date,
bo.order_estimated_delivery_date, 
boi.product_id,
boi.seller_id, 
boi.price,
boi.freight_value, 
boi.order_item_id,
DATEDIFF(day, order_purchase_timestamp, order_delivered_customer_date) AS days_to_delivery,
DATEDIFF(day, order_estimated_delivery_date, order_delivered_customer_date) AS days_late,
CASE 
WHEN order_delivered_customer_date IS NULL THEN NULL
WHEN DATEDIFF(day, order_estimated_delivery_date, order_delivered_customer_date) >=1 THEN 1
ELSE 0
END AS is_late
FROM {{ ref('bronze_order_items') }} as boi
LEFT JOIN {{ ref('bronze_orders') }} as bo
ON bo.order_id = boi.order_id
