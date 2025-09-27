{{ config(materialized='table') }}

select
    location_type_id,
    location_type_name,
    ingested_at
from {{ ref('stg_location_types') }}