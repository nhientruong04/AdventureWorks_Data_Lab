with numbers as (
    select top (datediff(day, '1990-01-01', '2050-12-31') + 1)
        row_number() over (order by (select null)) - 1 as n
    from sys.objects
),
date_spine as (
    select dateadd(day, n, cast('1990-01-01' as date)) as date_day
    from numbers
)

select
    cast(format(date_day, 'yyyyMMdd') as int) as DateKey,
    date_day as Date,
    year(date_day) as Year,
    month(date_day) as Month,
    day(date_day) as Day,
    datepart(quarter, date_day) as Quarter,
    case
        when month(date_day) in (12, 1, 2) then 'Winter'
        when month(date_day) in (3, 4, 5) then 'Spring'
        when month(date_day) in (6, 7, 8) then 'Summer'
        else 'Autumn'
    end as Season
from date_spine
