{{ config(materialized = 'table') }}

with base as (

    select distinct
        weather_code,
        case
            when precipitation_mm is null then 'none'
            when precipitation_mm < 2 then 'light'
            when precipitation_mm < 10 then 'moderate'
            else 'heavy'
        end                                   as precipitation_band,
        case
            when visibility_km is null then 'unknown'
            when visibility_km < 1  then 'very_low'
            when visibility_km < 5  then 'low'
            when visibility_km < 10 then 'medium'
            else 'high'
        end                                   as visibility_band
    from {{ ref('stg_weather') }}
),

final as (

    select
        md5(
            coalesce(weather_code, '') || '|' ||
            coalesce(precipitation_band, '') || '|' ||
            coalesce(visibility_band, '')
        )                                     as weather_key,
        weather_code,
        precipitation_band,
        visibility_band
    from base
)

select * from final;
