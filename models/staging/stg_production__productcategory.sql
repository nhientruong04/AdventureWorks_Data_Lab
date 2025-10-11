{{ config(materialized='ephemeral') }}
select
    cast(ProductCategoryID as int) as ProductCategoryID,
    cast(Name as varchar) as Category
from Production.ProductCategory
