SELECT
{{ dbt_utils.generate_surrogate_key(['padded_zip_code']) }} AS location_sk,
state,
city, 
padded_zip_code,
latitude,
longitude
FROM {{ ref('silver_geolocation') }}