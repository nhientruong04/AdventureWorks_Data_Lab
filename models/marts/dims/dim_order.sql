{{ config(
    cluster_by=["TerritoryName", "TerritoryGroup"]
) }}
WITH dups as (
    SELECT
        sales.SalesOrderID,
        sales.TerritoryName,
        sales.TerritoryGroup,
        sales.OnlineOrderFlag
    FROM {{ ref('int_sales__join') }} as sales
)

SELECT DISTINCT * FROM dups
