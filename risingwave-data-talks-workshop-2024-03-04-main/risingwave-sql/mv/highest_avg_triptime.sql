-- Question 1

-- Create a materialized view to compute the average, min and max trip time between each taxi zone.

-- From this MV, find the pair of taxi zones with the highest average trip time. You may need to use the dynamic filter pattern for this.

-- Bonus (no marks): Create an MV which can identify anomalies in the data. For example, if the average trip time between two zones is 1 minute, but the max trip time is 10 minutes and 20 minutes respectively.

-- Options:

--     Yorkville East, Steinway
--     Murray Hill, Midwood
--     East Flatbush/Farragut, East Harlem North
--     Midtown Center, University Heights/Morris Heights

-- Question 2

-- Recreate the MV(s) in question 1, to also find the number of trips for the pair of taxi zones with the highest average trip time.

-- Options:

--     5
--     3
--     10
--     1


DROP MATERIALIZED VIEW IF EXISTS ha_triptime_strange;
DROP MATERIALIZED VIEW IF EXISTS ha_triptime;

CREATE MATERIALIZED VIEW ha_triptime AS

SELECT
    trip_data.PULocationID as pickup_location_id,
    taxi_zone_pu.Zone as pickup_zone,
    trip_data.DOLocationID as dropoff_location_id,
    taxi_zone_do.Zone as dropoff_zone,
    avg(tpep_dropoff_datetime-tpep_pickup_datetime) as avg_triptime, 
    min(tpep_dropoff_datetime-tpep_pickup_datetime) as min_triptime, 
    max(tpep_dropoff_datetime-tpep_pickup_datetime) as max_triptime,
    count(*) cnt_trips
    
FROM
    trip_data
        JOIN taxi_zone as taxi_zone_pu
             ON trip_data.PULocationID = taxi_zone_pu.location_id
        JOIN taxi_zone as taxi_zone_do
             ON trip_data.DOLocationID = taxi_zone_do.location_id
GROUP BY 1,2,3,4;


CREATE MATERIALIZED VIEW ha_triptime_strange AS
with str_tr as(
    SELECT
    trip_data.*,
    taxi_zone_pu.Zone as pickup_zone,
    taxi_zone_do.Zone as dropoff_zone,
    (tpep_dropoff_datetime-tpep_pickup_datetime) as triptime,
    hat.avg_triptime, 
    case 
    when (tpep_dropoff_datetime-tpep_pickup_datetime)-INTERVAL '1' HOUR > hat.avg_triptime  then 1 
    when (tpep_dropoff_datetime-tpep_pickup_datetime)+INTERVAL '1' HOUR< hat.avg_triptime  then 1 
    else 0 end as anomaly
    
    
FROM
    trip_data
        JOIN taxi_zone as taxi_zone_pu
             ON trip_data.PULocationID = taxi_zone_pu.location_id
        JOIN taxi_zone as taxi_zone_do
             ON trip_data.DOLocationID = taxi_zone_do.location_id
        JOIN ha_triptime as hat
            on (trip_data.PULocationID = hat.pickup_location_id
                and trip_data.DOLocationID = hat.dropoff_location_id
           )
)

SELECT * FROM str_tr WHERE anomaly = 1;

