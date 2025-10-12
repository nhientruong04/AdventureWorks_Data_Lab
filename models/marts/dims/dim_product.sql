select
    prod.ProductID,
    prod.ProductName,
    prod.ProductModel,
    prod.ProductSize,
    prod.Color,
    prod.MakeFlag,
    prod.FinishedGoodsFlag,
    prod.Category,
    prod.SubCategory
from {{ ref('int_production__join') }} as prod;
