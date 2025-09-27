{{ config(materialized='view') }}

with raw as (
    select
        value:id::string             as location_type_id,
        value:name::string           as location_type_name,
        ingested_at
    from {{ source('api_raw', 'STG_LOCATION_TYPE_RAW') }},
         lateral flatten(input => payload:location_types)
)
select * from raw