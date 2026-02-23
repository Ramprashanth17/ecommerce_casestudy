SELECT
bp.*,
pct.PRODUCT_CATEGORY_NAME_ENGLISH AS product_name_english
FROM {{ ref('bronze_products')}} as bp
LEFT JOIN {{ ref('product_category_name_translation')}} as pct 
ON pct.PRODUCT_CATEGORY_NAME = bp.PRODUCT_CATEGORY_NAME