{{ config(materialized='view') }}

with raw as (
    select
        value:id::string             as machine_id,
        value:name::string           as machine_name,
        value:manufacturer::string   as manufacturer,
        value:year::int              as year,
        value:machine_type::string   as machine_type,
        value:machine_display::string as machine_display,
        value:opdb_id::string        as opdb_id,
        value:opdb_link::string      as opdb_link,
        value:opdb_img::string       as opdb_img,
        value:created_at::timestamp  as created_at,
        value:updated_at::timestamp  as updated_at,
        ingested_at
    from {{ source('api_raw', 'STG_MACHINES_RAW') }},
         lateral flatten(input => payload:machines)
)
select * from raw