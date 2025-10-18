SELECT
    orders.*,
    d.DateKey as SalesDateKey
FROM {{ ref('int_sales__join') }} as orders
LEFT JOIN {{ ref('dim_date') }} as d
    ON CAST(orders.OrderDate as DATE) = d.Date
