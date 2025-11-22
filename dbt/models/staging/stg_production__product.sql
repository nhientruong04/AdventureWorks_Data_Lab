SELECT
    CAST(ProductID as INT) as ProductID,
    CAST(Name as STRING) as ProductName,
    CAST(Color as STRING) as Color,
    CAST(Size as STRING) as ProductSize,
    CAST(ProductSubCategoryID as INT) ProductSubCategoryID,
    CAST(ProductModelID as INT) ProductModelID
FROM {{ source("Production", "Product") }}
