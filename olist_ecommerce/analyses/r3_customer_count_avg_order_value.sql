WITH order_totals as(
SELECT 
foi.order_sk,
foi.customer_fk,
gdl.state,
sum(foi.price) as total_order_price
FROM {{ ref('gold_fct_order_items') }} as foi
LEFT JOIN {{ ref('gold_dim_location') }} as gdl
ON gdl.location_sk = foi.location_fk
GROUP by foi.order_sk, foi.customer_fk, gdl.state
)
SELECT 
    state,
    count(distinct customer_fk) as total_customers,
    round(avg(total_order_price), 2) as avg_order_value
FROM order_totals
GROUP BY state
ORDER BY total_customers DESC;
