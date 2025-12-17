{{ config(
    materialized = 'incremental',
    unique_key   = 'traffic_event_id'
) }}

with source as (

    select *
    from {{ source('santa_clara_raw', 'TRAFFIC_RAW') }}

    {% if is_incremental() %}
        where observation_time > (select coalesce(max(observation_time), '1900-01-01') from {{ this }})
    {% endif %}
),

renamed as (

    select
        cast(traffic_event_id as varchar)      as traffic_event_id,
        observation_time,
        origin_id,
        destination_id,
        travel_time_min,
        congestion_level
    from source
)

select * from renamed;
