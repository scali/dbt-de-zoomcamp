{{
    config(
        materialized='table'
    )
}}

with green_tripdata as (
    select  pickup_locationid, 
            dropoff_locationid, 
            pickup_datetime, 
            dropoff_datetime,
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
), 
yellow_tripdata as (
    select  pickup_locationid, 
            dropoff_locationid, 
            pickup_datetime, 
            dropoff_datetime,
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
), 
fhv_tripdata as (
    select  pickup_locationid, 
            dropoff_locationid, 
            pickup_datetime, 
            dropoff_datetime,
        'FHV' as service_type
    from {{ ref('stg_fhv_tripdata') }}
), 
trips_unioned as (
    select * from green_tripdata
    union all 
    select * from yellow_tripdata
    union all 
    select * from fhv_tripdata
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select  service_type,
        {{ dbt_date.date_part("month", "pickup_datetime") }} as pickup_month,
        {{ dbt_date.date_part("month", "dropoff_datetime") }} as dropoff_month,
        pickup_locationid, 
        dropoff_locationid, 
        pickup_datetime, 
        dropoff_datetime, 
        pickup_zone.borough as pickup_borough, 
        pickup_zone.zone as pickup_zone, 
        dropoff_zone.borough as dropoff_borough, 
        dropoff_zone.zone as dropoff_zone
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid