{{ config(materialized='table') }}

select
    machine_id,
    machine_name,
    manufacturer,
    year,
    machine_type,
    machine_display,
    opdb_id,
    opdb_link,
    opdb_img,
    created_at,
    updated_at,
    ingested_at
from {{ ref('stg_machines') }}