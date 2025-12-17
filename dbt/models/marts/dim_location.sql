{{ config(materialized = 'table') }}

with base as (

    select distinct
        latitude,
        longitude,
        roadway_name,
        intersection_name,
        jurisdiction_code
    from {{ ref('stg_crashes') }}
),

final as (

    select
        md5(
            coalesce(cast(latitude as varchar), '') || '|' ||
            coalesce(cast(longitude as varchar), '') || '|' ||
            coalesce(roadway_name, '') || '|' ||
            coalesce(intersection_name, '')
        )                                        as location_key,
        latitude,
        longitude,
        roadway_name,
        intersection_name,
        jurisdiction_code
    from base
)

select * from final;
