{{ config(materialized='incremental',
unique_key='shipper_id'
) }}

with dist_shipper as (
    select distinct shipper_name as shipper_name
    from ls_dev.stg_shipping
)
select
{{ dbt_utils.surrogate_key(
      'shipper_name'
  ) }} as shipper_id,
coalesce(shipper_name, 'not defined') as shipper_name
 from dist_shipper