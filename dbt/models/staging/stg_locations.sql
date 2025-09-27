{{ config(materialized='view') }}

with raw as (

    select
        region_id,  -- passed through from ingestion
        value:id::string               as location_id,
        value:name::string             as location_name,
        value:city::string             as city,
        value:state::string            as state,
        value:country::string          as country,
        value:street::string           as street,
        value:zip::string              as zip,
        value:lat::float               as latitude,
        value:lon::float               as longitude,
        value:location_type_id::string as location_type_id,
        value:machine_count::int       as machine_count,
        value:created_at::timestamp    as created_at,
        value:updated_at::timestamp    as updated_at,
        ingested_at
    from {{ source('api_raw', 'STG_LOCATIONS_RAW') }},
         lateral flatten(input => payload:locations)

)

select * from raw