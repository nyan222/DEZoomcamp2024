${\color{blue}1.}$<br>
```shell
docker run --help | grep Automatically
```
${\color{blue}2.}$<br>
```shell
docker run -it --entrypoint=bash python:3.9
pip list
```
${\color{blue}3.}$<br>
``` sql
select count(*) 
from green_taxi_data 
where 
cast (lpep_pickup_datetime as date) = '2019-09-18'
and 
cast(lpep_dropoff_datetime as date) = '2019-09-18'
```

${\color{blue}4.}$<br>
``` sql
select cast(lpep_pickup_datetime as date) dt, 
       max(trip_distance) as largest_distance
from green_taxi_data
group by 1
order by 2 desc
limit 1
```

${\color{blue}5.}$<br>
``` sql
select coalesce(pickup."Borough", 'Unknown') pickup_zone,
       sum(total_amount) t_a
from green_taxi_data taxi
left join zones pickup on taxi."PULocationID" = pickup."LocationID"
left join zones dropoff on taxi."DOLocationID" = dropoff."LocationID"
WHERE cast (lpep_pickup_datetime as date) = '2019-09-18'
group by 1
order by 2 desc
limit 3
```

${\color{blue}6.}$<br>
``` sql
select coalesce(dropoff."Zone", 'Unknown') dropoff_zone
       max(tip_amount) largest_tip
from green_taxi_data taxi
left join zones pickup on taxi."PULocationID" = pickup."LocationID"
left join zones dropoff on taxi."DOLocationID" = dropoff."LocationID"
where pickup."Zone" = 'Astoria' and date_trunc('month',cast(lpep_pickup_datetime as date))='2019-09-01'
group by 1
order by 2 desc
limit 1
```
${\color{blue}7.}$<br>
```shell
terraform init
terraform apply
```
