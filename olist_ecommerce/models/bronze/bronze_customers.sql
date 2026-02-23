SELECT 
"customer_id" :: TEXT AS customer_id,
"customer_unique_id" :: TEXT AS customer_unique_id,
"customer_zip_code_prefix" :: TEXT AS customer_zip_code_prefix,
"customer_state" :: TEXT AS customer_state,
"customer_city" :: TEXT AS customer_city
FROM {{ source("RAW", "CUSTOMERS") }}