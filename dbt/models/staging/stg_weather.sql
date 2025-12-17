{{ config(
    materialized = 'incremental',
    unique_key   = 'weather_event_id'
) }}

with source as (

    select *
    from {{ source('santa_clara_raw', 'WEATHER_RAW') }}

    {% if is_incremental() %}
        where observation_time > (select coalesce(max(observation_time), '1900-01-01') from {{ this }})
    {% endif %}
),

renamed as (

    select
        cast(weather_event_id as varchar)      as weather_event_id,
        observation_time,
        cast(latitude as float)                as latitude,
        cast(longitude as float)               as longitude,
        weather_code,
        precipitation_mm,
        visibility_km,
        temperature_c
    from source
)

select * from renamed;
