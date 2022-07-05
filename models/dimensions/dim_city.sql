{{ config(materialized='incremental',
unique_key='city_id'
) }}

with dist_city as (
    select trim(split_part(lane,'->', 1)) as city_name from ls_dev.stg_shipping
    union
    select trim(split_part(lane,'->', 2)) as city_name from ls_dev.stg_shipping
)
select
{{ dbt_utils.surrogate_key(
      'city_name'
  ) }} as city_id,
coalesce(city_name, 'not defined') as city_name
 from dist_city