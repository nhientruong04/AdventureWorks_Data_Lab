{{ config(materialized='ephemeral') }}
SELECT
    CAST(ProductSubCategoryID as INT) as ProductSubCategoryID,
    CAST(ProductCategoryID as INT) as ProductCategoryID,
    CAST(Name as STRING) as SubCategory
FROM {{ source("Production", "ProductSubcategory")}}
