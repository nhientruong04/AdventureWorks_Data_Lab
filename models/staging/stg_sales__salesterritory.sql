{{ config(materialized='ephemeral') }}
SELECT
    CAST(TerritoryID as INT) as TerritoryID,
    CAST(Name as STRING) as TerritoryName,
    CAST("Group" as STRING) as TerritoryGroup
FROM Sales.SalesTerritory
