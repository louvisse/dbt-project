{{ config(materialized='incremental',
unique_key='lane_id'
) }}

with lanes as (
  select distinct
    trim(split_part(lane, '->', 1)) as from_city,
    trim(split_part(lane, '->', 2)) as to_city
  from
    ls_dev.stg_shipping
  )
select
{{ dbt_utils.surrogate_key(
      ['lanes.from_city', 'lanes.to_city']
  ) }} as lane_id,
  dim_from_city.city_id as from_city_id,
  dim_to_city.city_id as to_city_id
from lanes
inner join ls_dev.dim_city dim_from_city
on lanes.from_city = dim_from_city.city_name
inner join ls_dev.dim_city dim_to_city
on lanes.from_city = dim_to_city.city_name