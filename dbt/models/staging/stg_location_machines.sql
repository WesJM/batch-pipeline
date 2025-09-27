{{ config(materialized='view') }}

with raw as (

    select
        l.region_id,
        loc.value:id::string            as location_machine_xref_id,
        loc.value:location_id::string   as location_id,
        loc.value:machine_id::string    as machine_id,
        loc.value:created_at::timestamp as created_at,
        loc.value:updated_at::timestamp as updated_at,
        l.ingested_at
    from {{ source('api_raw', 'STG_LOCATIONS_RAW') }} l,
         lateral flatten(input => l.payload:locations) as loc_outer,
         lateral flatten(input => loc_outer.value:location_machine_xrefs) as loc

)

select * from raw