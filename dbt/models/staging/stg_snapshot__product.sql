WITH
join_subcategory AS (
    {{ join_snapshots(
        left_snapshot = ref('ProductSnapshot'),
        right_snapshot = ref('ProductSubcategorySnapshot'),
        left_key = 'ProductSubcategoryID',
        right_key = 'ProductSubcategoryID',
        left_valid_from = 'valid_from',
        left_valid_to = 'valid_to',
        right_valid_from = 'valid_from',
        right_valid_to = 'valid_to',
        right_attributes = ['Name', 'ProductCategoryID']
    ) }}
),

join_category AS (
    {{ join_snapshots(
        left_snapshot = 'join_subcategory',
        right_snapshot = ref('ProductCategorySnapshot'),
        left_key = 'ProductSubcategorySnapshot__ProductCategoryID',
        right_key = 'ProductCategoryID',
        left_valid_from = 'valid_from',
        left_valid_to = 'valid_to',
        right_valid_from = 'valid_from',
        right_valid_to = 'valid_to',
        right_attributes = ['Name']
    ) }}
),

join_model AS (
    {{ join_snapshots(
        left_snapshot = 'join_category',
        right_snapshot = ref('ProductModelSnapshot'),
        left_key = 'ProductModelID',
        right_key = 'ProductModelID',
        left_valid_from = 'valid_from',
        left_valid_to = 'valid_to',
        right_valid_from = 'valid_from',
        right_valid_to = 'valid_to',
        right_attributes = ['Name']
    ) }}
)

SELECT *
FROM join_model
