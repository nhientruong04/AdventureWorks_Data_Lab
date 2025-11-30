SELECT
    CAST(SalesOrderID AS INT) AS SalesOrderID,
    CAST(OrderDate AS DATETIME) AS OrderDate,
    CAST(OnlineOrderFlag AS INT) AS OnlineOrderFlag,
    CASE
        WHEN TerritoryID IS NULL THEN 0
        ELSE CAST(TerritoryID AS INT)
    END AS TerritoryID
FROM {{ source("Sales", "SalesOrderHeader") }}
