{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_location_machines') }}
),

fact as (
    select
        location_machine_xref_id,
        location_id,
        machine_id,
        region_id,
        created_at,
        updated_at,
        ingested_at
    from stg
)

select * from fact