with
category_subcategory as (
    select
        subcat.*,
        cat.Category
    from {{ ref('stg_production__productsubcategory') }} as subcat
    left join {{ ref('stg_production__productcategory') }} as cat
        on subcat.ProductCategoryID = cat.ProductCategoryID
),

model_join as (
    select
        p.*,
        pm.ProductModel
    from {{ ref('stg_production__product') }} as p
    left join {{ ref('stg_production__productmodel') }} as pm
        on p.ProductModelID = pm.ProductModelID
)

select
    mj.ProductID,
    mj.ProductName,
    mj.MakeFlag,
    mj.FinishedGoodsFlag,
    mj.ProductModel,
    mj.Color,
    mj.ProductSize,
    cs.Category,
    cs.SubCategory
from model_join as mj
left join category_subcategory as cs
    on mj.ProductSubCategoryID = cs.ProductSubCategoryID
