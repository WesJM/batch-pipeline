{{ config(materialized='table') }}

with locations as (
    select * from {{ ref('stg_locations') }}
),

deduped as (
    select
        location_id,
        region_id,
        location_type_id,
        location_name,
        city,
        state,
        country,
        street,
        zip,
        latitude,
        longitude,
        machine_count,
        max(created_at)  as created_at,
        max(updated_at)  as updated_at,
        max(ingested_at) as ingested_at
    from locations
    group by
        location_id,
        region_id,
        location_type_id,
        location_name,
        city,
        state,
        country,
        street,
        zip,
        latitude,
        longitude,
        machine_count
)

select * from deduped