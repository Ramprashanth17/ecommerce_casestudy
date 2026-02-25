SELECT 
gdl.state, 
count(case when foi.is_late = true THEN 1 END) as total_late_orders,
count(case when foi.order_status='delivered' THEN 1 END) as total_delivered_orders,
count(case when foi.is_late = true THEN 1 END) / 
nullif(count(case when foi.order_status='delivered' THEN 1 END), 0) * 100 as late_delivery_percentage
FROM {{ ref('gold_fct_order_items') }} as foi
LEFT JOIN {{ ref('gold_dim_location') }} as gdl
ON gdl.location_sk = foi.location_fk
group by gdl.state
order by round(late_delivery_percentage,2) desc;