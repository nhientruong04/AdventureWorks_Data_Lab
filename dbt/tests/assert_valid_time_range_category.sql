{{ config(tags=['scd']) }}

SELECT *
FROM {{ ref('ProductCategorySnapshot') }} AS cat
JOIN {{ ref('stg_snapshot__product') }} AS snaps
    ON cat.ProductCategoryID = snaps.ProductSubcategorySnapshot__ProductCategoryID
    AND cat.Name = snaps.ProductCategorySnapshot__Name
WHERE cat.valid_from > snaps.valid_from AND cat.valid_to < snaps.valid_to
