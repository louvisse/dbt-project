{{ config(materialized='incremental',
unique_key='sourcing_channel_id'
) }}

with dist_sourcing_channel as (
    select distinct sourcing_channel as sourcing_channel
    from ls_dev.stg_shipping
)
select
{{ dbt_utils.surrogate_key(
      'sourcing_channel'
  ) }} as sourcing_channel_id,
coalesce(sourcing_channel, 'not defined') as sourcing_channel
 from dist_sourcing_channel