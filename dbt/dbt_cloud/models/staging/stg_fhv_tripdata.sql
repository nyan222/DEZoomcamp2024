{{ config(materialized='view') }}

/*
Create a staging model for the fhv data, similar to the ones made for yellow and green data. 
Add an additional filter for keeping only records with pickup time in year 2019. 
Do not add a deduplication step. Run this models without limits (is_test_run: false). 
*/

select
    -- identifiers
	dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    SR_Flag, 
    Affiliated_base_number
from {{ source('staging','fhv_tripdata') }}
where extract(year from cast(pickup_datetime as timestamp)) = '2019'

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}