{{ config(
    materialized = 'incremental',
    unique_key   = 'crash_id'
) }}

with source as (

    select *
    from {{ source('santa_clara_raw', 'CRASHES_RAW') }}

    {% if is_incremental() %}
        where crash_datetime > (select coalesce(max(crash_datetime), '1900-01-01') from {{ this }})
    {% endif %}
),

renamed as (

    select
        cast(crash_id as varchar)              as crash_id,
        crash_datetime,
        cast(latitude as float)                as latitude,
        cast(longitude as float)               as longitude,
        roadway_name,
        intersection_name,
        jurisdiction_code,
        collision_type,
        severity_code,
        num_injured,
        num_killed,
        num_vehicles,
        lighting_condition,
        surface_condition
    from source
)

select * from renamed;
