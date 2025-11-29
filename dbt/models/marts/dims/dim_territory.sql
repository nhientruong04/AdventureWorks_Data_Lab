WITH
unknown AS (
    SELECT
        0 AS TerritoryKey,
        0 AS TerritoryID,
        'unknown' AS TerritoryName,
        'unknown' AS TerritoryGroup,
        'unknown' AS CountryRegionCode
),

final AS (
    SELECT * FROM unknown

    UNION ALL

    SELECT
    ROW_NUMBER() OVER (ORDER BY TerritoryID) AS TerritoryKey,
    *
    FROM {{ ref('stg_sales__salesterritory') }}
)

SELECT *
FROM final
