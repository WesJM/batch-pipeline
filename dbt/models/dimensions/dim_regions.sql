
{{ config(materialized='table') }}

with regions as (
    select * 
    from {{ ref('stg_regions') }}
),

deduped as (
    select
        region_id,
        region_name,
        region_full_name,
        state,
        latitude,
        longitude,
        effective_radius,
        -- metadata
        max(ingested_at)  as ingested_at
    from regions
    group by
        region_id,
        region_name,
        region_full_name,
        state,
        latitude,
        longitude,
        effective_radius
)

select * from deduped