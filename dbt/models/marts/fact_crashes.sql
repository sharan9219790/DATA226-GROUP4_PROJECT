{{ config(
    materialized = 'incremental',
    unique_key   = 'crash_id'
) }}

with crashes as (

    select *
    from {{ ref('stg_crashes') }}

    {% if is_incremental() %}
        where crash_datetime > (select coalesce(max(crash_datetime), '1900-01-01') from {{ this }})
    {% endif %}
),

loc_dim as (

    select
        location_key,
        latitude,
        longitude
    from {{ ref('dim_location') }}
),

crashes_with_loc as (

    select
        c.*,
        l.location_key
    from crashes c
    left join loc_dim l
      on c.latitude  = l.latitude
     and c.longitude = l.longitude
),

final as (

    select
        crash_id,
        date_trunc('day', crash_datetime)::date   as date_key,
        location_key,
        null::varchar                             as weather_key,
        null::varchar                             as traffic_key,
        severity_code,
        collision_type,
        num_injured,
        num_killed,
        num_vehicles,
        lighting_condition,
        surface_condition
    from crashes_with_loc
)

select * from final;
