SELECT
    prod.ProductID,
    prod.ProductName,
    prod.ProductModel,
    prod.ProductSize,
    prod.Color,
    prod.Category,
    prod.SubCategory
FROM {{ ref('int_production__join') }} as prod
