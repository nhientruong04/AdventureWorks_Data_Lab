SELECT
    CAST(SalesOrderID as INT) as SalesOrderID,
    CAST(ProductID as INT) as ProductID,
    CAST(OrderQty as INT) as OrderQty,
    CAST(UnitPrice as FLOAT64) as UnitPrice,
    CAST(UnitPriceDiscount as FLOAT64) as UnitPriceDiscount
FROM Sales.SalesOrderDetail
