{{ config(materialized='incremental',
unique_key='loadsmart_id'
) }}

select
loadsmart_id,
trim(split_part(lane, '->', 1))                   as lane_from,
d_lane_from.city_id as lane_from_id,
trim(split_part(lane, '->', 2))                   as lane_to,
d_lane_to.city_id as lane_to_id,
to_char(quote_date::date, 'YYYYMMDD')::decimal    as quote_date_key,
quote_date::timestamp as quote_date,
to_char(book_date::date, 'YYYYMMDD')::decimal     as book_date_key,
book_date::timestamp as book_date,
to_char(source_date::date, 'YYYYMMDD')::decimal   as source_date_key,
source_date::timestamp as source_date,
to_char(pickup_date::date, 'YYYYMMDD')::decimal   as pickup_date_key,
pickup_date::timestamp as pickup_date,
to_char(delivery_date::date, 'YYYYMMDD')::decimal as delivery_date_key,
delivery_date::timestamp as delivery_date,
book_price,
source_price,
pnl,
mileage,
stg.equipment_type,
eqp.equipment_type_id,
carrier_rating,
stg.sourcing_channel,
src.sourcing_channel_id,
vip_carrier,
carrier_dropped_us_count,
stg.carrier_name,
crr.carrier_id,
stg.shipper_name,
shp.shipper_id,
carrier_on_time_to_pickup,
carrier_on_time_to_delivery,
carrier_on_time_overall,
to_char(pickup_appointment_time::date, 'YYYYMMDD')::decimal as pickup_appointment_date_key,
pickup_appointment_time::timestamp as pickup_appointment_time,
to_char(delivery_appointment_time::date, 'YYYYMMDD')::decimal as delivery_appointment_date_key,
delivery_appointment_time::timestamp as delivery_appointment_time,
has_mobile_app_tracking,
has_macropoint_tracking,
has_edi_tracking,
contracted_load,
load_booked_autonomously,
load_sourced_autonomously,
load_was_cancelled
from
ls_dev.stg_shipping stg
inner join {{ ref('dim_city')}} d_lane_from
on d_lane_from.city_name = trim(split_part(lane, '->', 1))
inner join {{ ref('dim_city')}} d_lane_to
on d_lane_to.city_name = trim(split_part(lane, '->', 2))
inner join {{ ref('dim_equipment_type')}} eqp
on eqp.equipment_type = stg.equipment_type
inner join {{ ref('dim_sourcing_channel')}} src
on src.sourcing_channel = stg.sourcing_channel
inner join {{ ref('dim_carrier')}} crr
on crr.carrier_name = stg.carrier_name
inner join {{ ref('dim_shipper')}} shp
on shp.shipper_name = stg.shipper_name
