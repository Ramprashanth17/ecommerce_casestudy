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
), padded_zip_code AS (
    SELECT *, 
    CASE 
    WHEN LENGTH(customer_zip_code_prefix::TEXT) != 5
    THEN LPAD(customer_zip_code_prefix, 5, '0')
    ELSE customer_zip_code_prefix::TEXT
    END AS padded_zip_code
    FROM unique_customer_id
)
SELECT
customer_unique_id, 
customer_city,
customer_state,
padded_zip_code
FROM padded_zip_code
WHERE ROW_NUM = 1