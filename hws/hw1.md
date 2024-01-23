<html><body>
<span style="color: blue">1.</span><br>
docker run --help | grep Automatically<br>  
<br>
<br>
2. <br>
docker run -it --entrypoint=bash python:3.9<br>
pip list<br>
<br>
<br>
3.<br>
select count(*) <br>
from green_taxi_data <br>
where <br>
cast (lpep_pickup_datetime as date) = '2019-09-18'<br>
and <br>
cast(lpep_dropoff_datetime as date) = '2019-09-18'<br>
<br>
<br>
4.<br>
select cast(lpep_pickup_datetime as date) dt, <br>
       max(trip_distance) as largest_distance<br>
from green_taxi_data<br>
group by 1<br>
order by 2 desc<br>
limit 1<br>
<br>
<br>
5.<br>
select coalesce(pickup."Borough", 'Unknown') pickup_zone,<br>
       sum(total_amount) t_a<br>
from green_taxi_data taxi<br>
left join zones pickup on taxi."PULocationID" = pickup."LocationID"<br>
left join zones dropoff on taxi."DOLocationID" = dropoff."LocationID"<br>
WHERE cast (lpep_pickup_datetime as date) = '2019-09-18'<br>
group by 1<br>
order by 2 desc<br>
limit 3<br>
<br>
<br>
6.<br>
select coalesce(dropoff."Zone", 'Unknown') dropoff_zone,<br>
       max(tip_amount) largest_tip<br>
from green_taxi_data taxi<br>
left join zones pickup on taxi."PULocationID" = pickup."LocationID"<br>
left join zones dropoff on taxi."DOLocationID" = dropoff."LocationID"<br>
where pickup."Zone" = 'Astoria' and date_trunc('month',cast(lpep_pickup_datetime as date))='2019-09-01'<br>
group by 1<br>
order by 2 desc<br>
limit 1<br>
<br>
<br>
7.<br>
terraform init<br>
terraform apply<br>
</body></html>
