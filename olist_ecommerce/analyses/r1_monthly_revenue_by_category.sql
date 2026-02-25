SELECT 
    d.year,
    d.month,
    p.product_name_english,
    SUM(f.price) AS product_revenue,
    SUM(f.freight_value) AS freight_revenue
FROM {{ ref('gold_fct_order_items') }} f
LEFT JOIN {{ ref('gold_dim_date') }} d ON f.date_fk = d.date_sk
LEFT JOIN {{ ref('gold_dim_products') }} p ON f.product_fk = p.product_sk
GROUP BY 1, 2, 3
ORDER BY 1, 2, 4 DESC;
