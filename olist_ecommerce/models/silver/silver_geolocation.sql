WITH padded_zipcode AS (
    SELECT 
    *,
    CASE 
    WHEN LENGTH(zip_code_prefix) != 5
    THEN LPAD(zip_code_prefix, 5, '0')
    ELSE zip_code_prefix
    END AS padded_zip_code
    FROM {{ ref('bronze_geolocation') }}
), deduplicated_zip AS (
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY padded_zip_code ORDER BY latitude, longitude) AS representative_zipcode
    FROM padded_zipcode
)

SELECT 
state,
city,
padded_zip_code,
latitude,
longitude
FROM deduplicated_zip
WHERE representative_zipcode = 1