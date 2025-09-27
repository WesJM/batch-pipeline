{{ config(materialized='view') }}

with raw as (
    select
        value:id::string              as region_id,
        value:name::string            as region_name,
        value:full_name::string       as region_full_name,
        value:lat::float              as latitude,
        value:lon::float              as longitude,
        value:state::string           as state,
        value:effective_radius::float as effective_radius,
        ingested_at
    from {{ source('api_raw', 'STG_REGIONS_RAW') }},
         lateral flatten(input => payload:regions)
)

select * from raw