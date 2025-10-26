{{ config(materialized='ephemeral') }}
SELECT
    CAST(ProductCategoryID as INT) as ProductCategoryID,
    CAST(Name as STRING) as Category
FROM {{ source("Production", "ProductCategory") }}
