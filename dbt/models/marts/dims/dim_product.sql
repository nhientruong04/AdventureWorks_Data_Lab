SELECT
    {{ dbt_utils.generate_surrogate_key([
            'ProductID',
            'Name',
            'Size',
            'Color',
            'ProductModelSnapshot__Name',
            'ProductCategorySnapshot__Name',
            'ProductSubcategorySnapshot__Name',
            'DiscontinuedDate'
        ])
    }} AS ProductKey,
    CAST(ProductID AS INT) AS ProductID,
    CAST(Name AS STRING) AS Name,
    CAST(`Size` AS STRING) AS ProductSize,
    CAST(Color as STRING) as Color,
    CAST(ProductModelSnapshot__Name AS STRING) AS Model,
    CAST(ProductCategorySnapshot__Name AS STRING) AS Category,
    CAST(ProductSubcategorySnapshot__Name AS STRING) AS Subcategory,
    CAST(DiscontinuedDate AS DATETIME) AS DiscontinuedDate,
    valid_from AS ValidFrom,
    valid_to AS ValidTo,
    CASE
        WHEN CAST("{{ var('future-date') }}" AS DATETIME) = valid_to THEN 1
        ELSE 0
    END AS IsCurrentFlag
FROM {{ ref('stg_snapshot__product') }}
