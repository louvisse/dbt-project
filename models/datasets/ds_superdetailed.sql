{{ config(materialized='incremental',
unique_key='loadsmart_id'
) }}

select
fromcity.city_name from_city_name,
tocity.city_name to_city_name,
quotedate.date_actual quote_date,
bookdate.date_actual book_date,
sourcedate.date_actual source_date,
pickupdate.date_actual pickup_date,
deliverydate.date_actual delivery_date,
pickupappdate.date_actual pickupappointment_date,
deliveryappdate.date_actual deliveryappointment_date,
eqp.equipment_type,
src.sourcing_channel,
crr.carrier_name,
shr.shipper_name,
fact.*

from {{ ref('fact_shipping')}} fact
inner join {{ ref('dim_city')}} fromcity
on fact.lane_from_id = fromcity.city_id
inner join {{ ref('dim_city')}} tocity
on fact.lane_to_id = tocity.city_id
inner join ls_dev.dim_date quotedate
on fact.quote_date_key = quotedate.date_dim_id
inner join ls_dev.dim_date bookdate
on fact.book_date_key = bookdate.date_dim_id
inner join ls_dev.dim_date sourcedate
on fact.source_date_key = sourcedate.date_dim_id
inner join ls_dev.dim_date pickupdate
on fact.pickup_date_key = pickupdate.date_dim_id
inner join ls_dev.dim_date deliverydate
on fact.delivery_date_key = deliverydate.date_dim_id
inner join ls_dev.dim_date pickupappdate
on fact.pickup_appointment_date_key = pickupappdate.date_dim_id
inner join ls_dev.dim_date deliveryappdate
on fact.delivery_appointment_date_key = deliveryappdate.date_dim_id
inner join {{ ref('dim_equipment_type')}} eqp
on fact.equipment_type_id = eqp.equipment_type_id
inner join {{ ref('dim_sourcing_channel')}} src
on fact.sourcing_channel_id = src.sourcing_channel_id
inner join {{ ref('dim_carrier')}} crr
on fact.carrier_id = crr.carrier_id
inner join {{ ref('dim_shipper')}} shr
on fact.shipper_id = shr.shipper_id