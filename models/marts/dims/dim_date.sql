-- Step 1: Generate the date spine
WITH date_spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('1990-01-01' as date)",
        end_date="cast('2050-12-31' as date)"
    ) }}

),

-- Step 2: Add useful date components
final as (
    SELECT
        CAST(format_date('%Y%m%d', date_day) as int64) as DateKey,
        date_day as Date,
        EXTRACT(year FROM date_day) as Year,
        EXTRACT(quarter FROM date_day) as Quarter,
        EXTRACT(month FROM date_day) as Month,
        EXTRACT(week FROM date_day) as Week,
        CASE
            WHEN EXTRACT(month FROM date_day) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(month FROM date_day) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(month FROM date_day) IN (6, 7, 8) THEN 'Summer'
            ELSE 'Autumn'
        END as Season
    FROM date_spine
)

SELECT * FROM final
