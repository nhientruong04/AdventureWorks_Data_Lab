SELECT
    CAST(SalesOrderID as INT) as SalesOrderID,
    CAST(ProductID as INT) as ProductID,
    ABS(CAST(OrderQty as INT)) as OrderQty,
    ABS(CAST(UnitPrice as FLOAT64)) as UnitPrice,
    ABS(CAST(LineTotal as FLOAT64)) as LineTotal
FROM Sales.SalesOrderDetail
WHERE OrderQty IS NOT NULL
    AND UnitPrice IS NOT NULL
    AND LineTotal IS NOT NULL
