with
sales_filtered as (
    select
        {{ select_except(ref('int_sales__join'), ['TerritoryID', 'TerritoryName', 'TerritoryGroup', 'OnlineOrderFlag']) }}
    from {{ ref('int_sales__join') }}
),

date_join as (
    select
        SalesOrderID,
        ProductID,
        SalesOrderDetailID,
        OrderQty,
        UnitPrice,
        UnitPriceDiscount,
        d.DateKey as SalesDateKey
    from sales_filtered
    left join {{ ref('dim_date') }} as d
        on cast(sales_filtered.OrderDate as date) = d.Date
)

select *
from date_join
