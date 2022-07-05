{{ config(materialized='incremental',
unique_key='carrier_id'
) }}

with dist_carrier_name as (
    select distinct carrier_name as carrier_name
    from ls_dev.stg_shipping
)
select
{{ dbt_utils.surrogate_key(
      'carrier_name'
  ) }} as carrier_id,
coalesce(carrier_name, 'not defined') as carrier_name
 from dist_carrier_name