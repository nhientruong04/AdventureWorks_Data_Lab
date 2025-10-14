SELECT
    CAST(SalesOrderID as INT) as SalesOrderID,
    CAST(OrderDate as DATE) as OrderDate,
    CAST(OnlineOrderFlag as INT) as OnlineOrderFlag,
    CAST(TerritoryID as INT) as TerritoryID
FROM Sales.SalesOrderHeader
