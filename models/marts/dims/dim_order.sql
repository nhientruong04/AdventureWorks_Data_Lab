select
    sales.SalesOrderID,
    sales.TerritoryName,
    sales.TerritoryGroup,
    sales.OnlineOrderFlag
from {{ ref('int_sales__join') }} as sales;
