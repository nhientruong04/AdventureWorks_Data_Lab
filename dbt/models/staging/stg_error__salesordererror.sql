SELECT
    CAST(SalesOrderID as INT) as SalesOrderID,
    CAST(ProductID as INT) as ProductID,
    CAST(OrderQty as INT) as OrderQty,
    CAST(UnitPrice as FLOAT64) as UnitPrice,
    CAST(LineTotal as FLOAT64) as LineTotal
FROM Sales.SalesOrderDetail
WHERE OrderQty IS NULL
    OR UnitPrice IS NULL
    OR LineTotal IS NULL
    OR LineTotal = 0
