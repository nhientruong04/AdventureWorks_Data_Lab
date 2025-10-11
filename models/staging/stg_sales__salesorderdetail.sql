select
    cast(SalesOrderID as int) as SalesOrderID,
    cast(ProductID as int) as ProductID,
    cast(SalesOrderDetailID as int) as SalesOrderDetailID,
    cast(OrderQty as int) as OrderQty,
    cast(UnitPrice as float) as UnitPrice,
    cast(UnitPriceDiscount as float) as UnitPriceDiscount
from Sales.SalesOrderDetail
