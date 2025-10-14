WITH
sales_filtered as (
    SELECT
        * EXCEPT (TerritoryID, TerritoryName, TerritoryGroup, OnlineOrderFlag)
    FROM {{ ref('int_sales__join') }}
),

final as (
    SELECT
        SalesOrderID,
        ProductID,
        OrderQty,
        UnitPrice,
        d.DateKey as SalesDateKey
    FROM sales_filtered
    LEFT JOIN {{ ref('dim_date') }} as d
        ON CAST(sales_filtered.OrderDate as DATE) = d.Date
)

SELECT *
FROM final
