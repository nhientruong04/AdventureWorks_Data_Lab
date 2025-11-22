WITH
category_subcategory as (
    SELECT
        subcat.*,
        cat.Category
    FROM {{ ref('stg_production__productsubcategory') }} as subcat
    LEFT JOIN {{ ref('stg_production__productcategory') }} as cat
        ON subcat.ProductCategoryID = cat.ProductCategoryID
),

model_join as (
    SELECT
        p.*,
        pm.ProductModel
    FROM {{ ref('stg_production__product') }} as p
    LEFT JOIN {{ ref('stg_production__productmodel') }} as pm
        ON p.ProductModelID = pm.ProductModelID
)

SELECT
    mj.ProductID,
    mj.ProductName,
    mj.ProductModel,
    mj.Color,
    mj.ProductSize,
    cs.Category,
    cs.SubCategory
FROM model_join as mj
LEFT JOIN category_subcategory as cs
    ON mj.ProductSubCategoryID = cs.ProductSubCategoryID
