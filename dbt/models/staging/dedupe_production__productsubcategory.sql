{{
    config(
        materialized="ephemeral",
        tags=["dedupe"]
    )
}}

WITH
hashed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
                'ProductCategoryID',
                'ProductSubcategoryID',
                'Name'
            ]) }} AS grain_id,
        *
    FROM {{ source("Production", "ProductSubcategory") }}
),

mark_real_diffs AS (
    SELECT
        *,
        CASE
            WHEN grain_id !=
                COALESCE(
                    LAG(grain_id) OVER (PARTITION BY ProductCategoryID ORDER BY ModifiedDate),
                    'first_row'
                ) THEN 1
            ELSE 0
        END AS is_real_diff
    FROM hashed
),

final AS (
    SELECT
        * EXCEPT(grain_id, is_real_diff),
        grain_id AS _dedupe_grain_id
    FROM mark_real_diffs
    WHERE is_real_diff = 1
)

SELECT * FROM final
