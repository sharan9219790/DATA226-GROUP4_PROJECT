{{ config(materialized = 'table') }}

with base as (

    select distinct
        congestion_level,
        case
            when travel_time_min is null then 'unknown'
            when travel_time_min < 10 then 'short'
            when travel_time_min < 30 then 'medium'
            else 'long'
        end                                   as travel_time_band
    from {{ ref('stg_traffic') }}
),

final as (

    select
        md5(
            coalesce(congestion_level, '') || '|' ||
            coalesce(travel_time_band, '')
        )                                     as traffic_key,
        congestion_level,
        travel_time_band
    from base
)

select * from final;
