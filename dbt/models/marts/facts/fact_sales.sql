{{
    config(
        unique_key=["ProductID", "SalesOrderID"],
        cluster_by=["ProductID", "SalesOrderID"]
    )
}}
SELECT
    * EXCEPT (OrderDate, ModifiedDate),
    {{ convert_datekey('ModifiedDate') }} as ModifiedDateKey,
    {{ convert_datekey('OrderDate') }} as OrderDateKey
FROM {{ ref('int_sales__join') }}
{% if is_incremental() %}
    WHERE {{ convert_datekey('ModifiedDate') }} >= (SELECT MAX(ModifiedDateKey) FROM {{ this }})
{% endif %}
