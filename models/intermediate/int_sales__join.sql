with
header_territory_joined as (
    select
        sales_header.*,
        sales_terr.TerritoryName,
        sales_terr.TerritoryGroup
    from {{ ref('stg_sales__salesorderheader') }} as sales_header
    left join {{ ref('stg_sales__salesterritory') }} as sales_terr
        on sales_header.TerritoryID = sales_terr.TerritoryID
)

select
    sales_details.*,
    hj.OrderDate,
    hj.OnlineOrderFlag,
    hj.TerritoryID,
    hj.TerritoryName,
    hj.TerritoryGroup
from {{ ref('stg_sales__salesorderdetail') }} as sales_details
left join header_territory_joined as hj
    on sales_details.SalesOrderID = hj.SalesOrderID
