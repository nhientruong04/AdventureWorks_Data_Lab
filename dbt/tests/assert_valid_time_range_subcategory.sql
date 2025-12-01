{{ config(tags=['scd']) }}

SELECT *
FROM {{ ref('ProductSubcategorySnapshot') }} AS subcat
JOIN {{ ref('stg_snapshot__product') }} AS snaps
    ON subcat.ProductSubcategoryID = snaps.ProductSubcategoryID
    AND subcat.Name = snaps.ProductSubcategorySnapshot__Name
    AND subcat.ProductCategoryID = snaps.ProductSubcategorySnapshot__ProductCategoryID
WHERE subcat.valid_from > snaps.valid_from AND subcat.valid_to < snaps.valid_to
