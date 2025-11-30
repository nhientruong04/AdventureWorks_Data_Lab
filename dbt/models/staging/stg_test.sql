WITH
mock_data AS (
    SELECT
        CAST('9999-12-31 23:59:59' AS DATETIME) AS valid_to,
        -- Airbyte Columns (Mocked)
        CAST('mock_airbyte_id_new' AS STRING) AS _airbyte_raw_id,
        CAST('2025-11-30 06:30:00+00' AS TIMESTAMP) AS _airbyte_extracted_at,
        JSON_OBJECT("mock_key", "mock_value") AS _airbyte_meta,
        CAST(1001 AS INT64) AS _airbyte_generation_id,

        -- Source Columns (Mocked for a new product)
        CAST('Bib-Shorts' AS STRING) AS Name,
        CAST('00000000-0000-0000-0000-000000000123' AS STRING) AS rowguid,
        CAST('2009-01-01 13:00:00' AS DATETIME) AS ModifiedDate,
        CAST(2 AS INT64) AS ProductCategoryID,
        CAST(18 AS INT64) AS ProductSubcategoryID,

        -- SCD/Dedupe Columns (Set to NULL as they are calculated later)
        CAST(NULL AS STRING) AS _dedupe_grain_id,
        CAST(NULL AS STRING) AS dbt_scd_id,
        CAST(NULL AS DATETIME) AS dbt_updated_at,
        CAST('2009-01-01 13:00:00' AS DATETIME) AS valid_from
),

existing_data AS (
    SELECT *
    FROM `argon-triode-474919-k7`.`dev_andrew_snapshots`.`ProductSubcategorySnapshot`
),


modified AS (
    SELECT
        CASE
            WHEN rowguid = '67B58D2B-5798-4A90-8C6C-5DDACF057171' THEN CAST('2009-01-01 13:00:00' AS DATETIME)
            ELSE valid_to
        END AS valid_to,
        * EXCEPT(valid_to)
    FROM existing_data
),

spine AS (
    SELECT *
    FROM `argon-triode-474919-k7`.`dev_andrew_snapshots`.`ProductSnapshot` AS p
),

dataset AS (
    SELECT * FROM modified
    UNION ALL
    SELECT * FROM mock_data
),

dataset_2 AS (
    SELECT *
    FROM `argon-triode-474919-k7`.`dev_andrew_snapshots`.`ProductCategorySnapshot` AS p
),

join_subcategory AS (
    {{ join_snapshots(
        left_snapshot = 'spine',
        right_snapshot = 'dataset',
        left_key = 'ProductSubcategoryID',
        right_key = 'ProductSubcategoryID',
        left_valid_from = 'valid_from',
        left_valid_to = 'valid_to',
        right_valid_from = 'valid_from',
        right_valid_to = 'valid_to',
        right_attributes = ['Name', 'ProductCategoryID']
    ) }}
),

final AS (
    {{ join_snapshots(
        left_snapshot = 'join_subcategory',
        right_snapshot = 'dataset_2',
        left_key = 'dataset__ProductCategoryID',
        right_key = 'ProductCategoryID',
        left_valid_from = 'valid_from',
        left_valid_to = 'valid_to',
        right_valid_from = 'valid_from',
        right_valid_to = 'valid_to',
        right_attributes = ['Name']
    ) }}
)

SELECT
    ProductID,
    ProductSubcategoryID,
    dataset__ProductCategoryID,
    dataset__Name,
    dataset_2__Name,
    valid_from,
    valid_to
FROM final
