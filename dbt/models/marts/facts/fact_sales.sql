{{
    config(
        unique_key=["ProductKey", "SalesOrderID"],
        cluster_by=["ProductKey", "SalesOrderID"]
    )
}}

WITH
new_records AS (
    SELECT *
    FROM {{ ref('int_sales__join') }}
    {% if is_incremental() %}
        WHERE {{ convert_datekey('OrderDate') }} >= (SELECT MAX(OrderDateKey)-7 FROM {{ this }})
    {% endif %}
),

product_join AS (
    SELECT
        new_records.* EXCEPT(ProductID),
        prod.ProductKey
    FROM new_records
    JOIN {{ ref('dim_product') }} AS prod
        ON new_records.ProductID = prod.ProductID
        AND new_records.OrderDate >= prod.ValidFrom
        AND new_records.OrderDate < prod.ValidTo
),

terr_join AS (
    SELECT
        p.* EXCEPT(TerritoryID),
        t.TerritoryKey
    FROM product_join AS p
    JOIN {{ ref('dim_territory') }} AS t
        ON p.TerritoryID = t.TerritoryID
),

final AS (
    SELECT
        * EXCEPT(OrderDate),
        {{ convert_datekey('OrderDate') }} AS OrderDateKey,
        CURRENT_DATETIME() AS LoadDatetime,
    FROM terr_join
)

SELECT * FROM final
