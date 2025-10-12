WITH numbers AS (
    SELECT TOP (DATEDIFF(DAY, '1990-01-01', '2050-12-31') + 1)
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
    FROM sys.all_objects AS a
    CROSS JOIN sys.all_objects AS b
),

-- Step 2: Convert to continuous date range
date_spine AS (
    SELECT DATEADD(DAY, n, CAST('1990-01-01' AS DATE)) AS date_day
    FROM numbers
),

-- Step 3: Add useful date attributes
final AS (
    SELECT
        CAST(FORMAT(date_day, 'yyyyMMdd') AS INT) AS DateKey,
        CAST(date_day AS DATE) AS [Date],
        DATEPART(YEAR, date_day) AS [Year],
        DATEPART(QUARTER, date_day) AS [Quarter],
        DATEPART(MONTH, date_day) AS [Month],
        DATEPART(DAY, date_day) AS [Day],
        CASE
            WHEN MONTH(date_day) IN (12, 1, 2) THEN 'Winter'
            WHEN MONTH(date_day) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(date_day) IN (6, 7, 8) THEN 'Summer'
            ELSE 'Autumn'
        END AS [Season]
    FROM date_spine
)

-- Step 4: Select the final result
SELECT *
FROM final;
