SELECT
    sales_details.*,
    sh.* EXCEPT(SalesOrderID)
FROM {{ ref('stg_sales__salesorderdetail') }} as sales_details
LEFT JOIN {{ ref('stg_sales__salesorderheader') }} as sh
    ON sales_details.SalesOrderID = sh.SalesOrderID

{% if target.name == 'dev' %}

ORDER BY sh.OrderDate
LIMIT 10000

{% endif %}
