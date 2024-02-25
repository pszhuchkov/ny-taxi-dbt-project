{{ config(materialized='table') }}

with fhv_trips as (
    select *, 
        'FHV' as service_type 
    from {{ ref('stg_fhv_tripdata') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    dispatching_base_num,
    cast(pickup_datetime as timestamp) pickup_datetime,
    cast(dropoff_datetime as timestamp) dropoff_datetime,
    cast(pickup_locationid as integer) as  pickup_locationid,
    cast(dropoff_locationid as integer) as dropoff_locationid,
    cast(sr_flag as integer) as sr_flag,
    affiliated_base_number,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone
from fhv_trips
inner join dim_zones as pickup_zone
on fhv_trips.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_trips.dropoff_locationid = dropoff_zone.locationid
