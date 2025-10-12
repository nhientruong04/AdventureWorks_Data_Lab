{{ config(materialized='ephemeral') }}
select
    cast(ProductSubCategoryID as int) as ProductSubCategoryID,
    cast(ProductCategoryID as int) as ProductCategoryID,
    cast(Name as varchar) as SubCategory
from Production.ProductSubcategory
