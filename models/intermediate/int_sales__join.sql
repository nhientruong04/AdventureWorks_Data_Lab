SELECT
    sales_details.*,
    hj.OrderDate,
    hj.OnlineOrderFlag,
    hj.TerritoryID,
    hj.TerritoryName,
    hj.TerritoryGroup
FROM {{ ref('stg_sales__salesorderdetail') }} as sales_details
LEFT JOIN {{ ref('int_sales__salesheader_join') }} as hj
    ON sales_details.SalesOrderID = hj.SalesOrderID
{% if target.name == 'dev' %}
ORDER BY hj.OrderDate
LIMIT 10000
{% endif %}
