{{ config(tags=['scd']) }}

SELECT *
FROM {{ ref('ProductModelSnapshot') }} AS model
JOIN {{ ref('stg_snapshot__product') }} AS snaps
    ON model.ProductModelID = snaps.ProductModelID
    AND model.Name = snaps.ProductModelSnapshot__Name
WHERE model.valid_from > snaps.valid_from AND model.valid_to < snaps.valid_to
