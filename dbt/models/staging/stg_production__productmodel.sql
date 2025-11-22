SELECT
    CAST(ProductModelID as INT) as ProductModelID,
    CAST(Name as STRING) as ProductModel
FROM {{ source("Production", "ProductModel") }}
