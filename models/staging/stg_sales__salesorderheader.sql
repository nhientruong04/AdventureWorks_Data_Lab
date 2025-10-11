select
    cast(SalesOrderID as int) as SalesOrderID,
    cast(OrderDate as date) as OrderDate,
    cast(OnlineOrderFlag as int) as OnlineOrderFlag,
    cast(TerritoryID as int) as TerritoryID
from Sales.SalesOrderHeader
