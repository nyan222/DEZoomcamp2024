{{ config(materialized='view') }}

/*
Create a staging model for the fhv data, similar to the ones made for yellow and green data. 
Add an additional filter for keeping only records with pickup time in year 2019. 
Do not add a deduplication step. Run this models without limits (is_test_run: false). 
*/
with t as (
select
    -- identifiers
	  dispatching_base_num,
    cast(pickup_datetime  as timestamp) as pickup_datetime,
    cast(drop_off_datetime as timestamp) as dropoff_datetime,
    cast(p_ulocation_id as integer) as  pickup_locationid,
    cast(d_olocation_id as integer) as dropoff_locationid,
    sr_flag, 
    affiliated_base_number
from {{ source('staging','fhv_tripdata') }}
)
select * from t
where extract(year from pickup_datetime) = 2019
--where {{ dbt_date.date_part("year", "pickup_datetime") }} = 2019

-- dbt build --m <model.sql> --var 'is_test_run: false'
-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}