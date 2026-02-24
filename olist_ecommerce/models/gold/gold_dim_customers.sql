SELECT
{{ dbt_utils.generate_surrogate_key(['customer_unique_id']) }} as customer_sk,
sc.customer_unique_id,
gl.location_sk
FROM {{ ref('silver_customers') }} as sc 
LEFT JOIN {{ ref('gold_dim_location') }} as gl 
on sc.padded_zip_code = gl.padded_zip_code