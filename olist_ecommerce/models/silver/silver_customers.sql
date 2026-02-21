WITH unique_customer_id AS
(
    SELECT 
    customer_id,
    customer_unique_id,
    customer_city,
    customer_zip_code_prefix,
    customer_state,
    ROW_NUMBER () OVER (PARTITION BY customer_unique_id ORDER BY customer_id DESC) AS ROW_NUM
    FROM {{ ref('bronze_customers') }}
)
SELECT
customer_unique_id, 
customer_city,
customer_state,
customer_zip_code_prefix
FROM unique_customer_id 
WHERE ROW_NUM = 1