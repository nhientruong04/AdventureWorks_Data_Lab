-- sales_header and territory join
SELECT
    sales_header.*,
    sales_terr.TerritoryName,
    sales_terr.TerritoryGroup
FROM {{ ref('stg_sales__salesorderheader') }} as sales_header
LEFT JOIN {{ ref('stg_sales__salesterritory') }} as sales_terr
    ON sales_header.TerritoryID = sales_terr.TerritoryID
