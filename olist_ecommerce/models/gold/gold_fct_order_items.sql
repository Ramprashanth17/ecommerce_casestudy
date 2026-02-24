SELECT
{{ dbt_utils.generate_surrogate_key(['order_id', 'order_item_id']) }} as order_sk,
soi.order_id,
soi.order_item_id,
soi.price,
soi.freight_value,
soi.order_status,
soi.days_to_delivery,
soi.days_late,
soi.is_late,
soi.order_estimated_delivery_date,
soi.order_delivered_customer_date,
gdp.product_sk as product_fk,
gdc.location_sk as location_fk,
gdd.date_sk as date_fk,
gdc.customer_sk as customer_fk
FROM {{ ref('silver_order_items_enriched') }} as soi
LEFT JOIN {{ ref('gold_dim_products') }} as gdp 
ON gdp.product_id = soi.product_id
LEFT JOIN {{ ref('bronze_customers') }} as bc 
ON bc.customer_id = soi.customer_id
LEFT JOIN {{ ref('gold_dim_customers') }} as gdc
ON gdc.customer_unique_id = bc.customer_unique_id
LEFT JOIN {{ ref('gold_dim_date') }} as gdd
ON gdd.date_day = soi.order_purchase_timestamp::DATE