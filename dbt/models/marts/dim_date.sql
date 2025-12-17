{{ config(materialized = 'table') }}

with dates as (

    select
        dateadd(day, row_number() over (order by 0) - 1, to_date('2010-01-01')) as date_day
    from table(generator(rowcount => 8000))
),

final as (

    select
        date_day                           as date_key,
        year(date_day)                     as year,
        month(date_day)                    as month,
        day(date_day)                      as day,
        to_char(date_day, 'DY')            as day_of_week_short,
        to_char(date_day, 'DAY')           as day_of_week,
        case
            when day_of_week in ('SATURDAY','SUNDAY') then 1
            else 0
        end                                as is_weekend
    from dates
    where date_day between to_date('2010-01-01') and to_date('2030-12-31')
)

select * from final;
