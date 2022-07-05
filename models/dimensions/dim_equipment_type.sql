{{ config(materialized='incremental',
unique_key='equipment_type_id'
) }}

with dist_equip_type as (
    select distinct equipment_type as equipment_type
    from ls_dev.stg_shipping
)
select
{{ dbt_utils.surrogate_key(
      'equipment_type'
  ) }} as equipment_type_id,
coalesce(equipment_type, 'not defined') as equipment_type
 from dist_equip_type