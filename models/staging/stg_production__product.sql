select
    cast(ProductID as int) as ProductID,
    cast(Name as varchar) as ProductName,
    cast(MakeFlag as int) as MakeFlag,
    cast(FinishedGoodsFlag as int) as FinishedGoodsFlag,
    cast(Color as varchar) as Color,
    cast("Size" as varchar) as ProductSize,
    cast(ProductSubCategoryID as int) ProductSubCategoryID,
    cast(ProductModelID as int) ProductModelID
from Production.Product
