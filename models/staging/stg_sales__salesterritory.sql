{{ config(materialized='ephemeral') }}
select
    cast(TerritoryID as int) as TerritoryID,
    cast(Name as varchar) as TerritoryName,
    cast("Group" as varchar) as TerritoryGroup
from Sales.SalesTerritory
