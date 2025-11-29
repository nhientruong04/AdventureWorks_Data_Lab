WITH date_spine AS (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="DATE('1990-01-01')",
        end_date="DATE('2050-12-31')"
    ) }}

),

final AS (
    SELECT
        CAST(format_date('%Y%m%d', date_day) AS INT) AS DateKey,
        date_day AS Date,
        EXTRACT(day FROM date_day) AS Day,
        EXTRACT(week FROM date_day) AS Week,
        EXTRACT(month FROM date_day) AS Month,
        EXTRACT(quarter FROM date_day) AS Quarter,
        CASE
            WHEN EXTRACT(month FROM date_day) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(month FROM date_day) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(month FROM date_day) IN (6, 7, 8) THEN 'Summer'
            ELSE 'Autumn'
        END AS Season,
        EXTRACT(year FROM date_day) AS Year
    FROM date_spine
)

SELECT * FROM final
