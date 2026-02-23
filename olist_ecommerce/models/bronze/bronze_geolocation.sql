SELECT
"geolocation_zip_code_prefix" :: TEXT AS zip_code_prefix,
"geolocation_lat" :: FLOAT AS latitude,
"geolocation_lng" :: FLOAT AS longitude,
"geolocation_state" :: TEXT AS state,
"geolocation_city" :: TEXT AS city,
FROM {{ source('RAW', 'GEOLOCATION')}}