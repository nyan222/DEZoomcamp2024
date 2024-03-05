# 1 2

```sql
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
```

```bash
dev=> select * from ha_triptime order by avg_triptime desc limit 3;
 pickup_location_id |          pickup_zone           | dropoff_location_id |    dropoff_zone    | avg_triptime | min_triptime | max_triptime | cnt_trips 
--------------------+--------------------------------+---------------------+--------------------+--------------+--------------+--------------+-----------
                262 | Yorkville East                 |                 223 | Steinway           | 23:59:33     | 23:59:33     | 23:59:33     |         1
                224 | Stuy Town/Peter Cooper Village |                 171 | Murray Hill-Queens | 23:58:44     | 23:58:44     | 23:58:44     |         1
                243 | Washington Heights North       |                 120 | Highbridge Park    | 23:58:40     | 23:58:40     | 23:58:40     |         1
(3 rows)



dev=> select * from ha_triptime_strange limit 10;
 vendorid | tpep_pickup_datetime | tpep_dropoff_datetime | passenger_count | trip_distance | ratecodeid | store_and_fwd_flag | pulocationid | dolocationid | payment_type | fare_amount | extra | mta_tax | tip_amount | tolls_amount | improvement_surcharge | total_amount | congestion_surcharge | airport_fee |          pickup_zone          |       dropoff_zone        | triptime |  avg_triptime   | anomaly 
----------+----------------------+-----------------------+-----------------+---------------+------------+--------------------+--------------+--------------+--------------+-------------+-------+---------+------------+--------------+-----------------------+--------------+----------------------+-------------+-------------------------------+---------------------------+----------+-----------------+---------
        2 | 2022-01-01 02:33:08  | 2022-01-02 02:25:03   |               1 |          2.34 |          1 | N                  |          170 |           50 |            2 |        13.5 |   0.5 |     0.5 |          0 |            0 |                   0.3 |         17.3 |                  2.5 |           0 | Murray Hill                   | Clinton West              | 23:51:55 | 01:11:40.84     |       1
        2 | 2022-01-01 14:58:12  | 2022-01-02 14:56:28   |               1 |          1.72 |          1 | N                  |          142 |          141 |            1 |         8.5 |     0 |     0.5 |       1.18 |            0 |                   0.3 |        12.98 |                  2.5 |           0 | Lincoln Square East           | Lenox Hill West           | 23:58:16 | 00:59:22.663793 |       1
        2 | 2022-01-01 00:55:03  | 2022-01-01 01:18:21   |               1 |          4.08 |          1 | N                  |          263 |           50 |            1 |        17.5 |   0.5 |     0.5 |       4.26 |            0 |                   0.3 |        25.56 |                  2.5 |           0 | Yorkville West                | Clinton West              | 00:23:18 | 02:53:58        |       1
        2 | 2022-01-01 21:08:28  | 2022-01-02 00:00:00   |               1 |          1.07 |          1 | N                  |          158 |          211 |            1 |           6 |   0.5 |     0.5 |       1.96 |            0 |                   0.3 |        11.76 |                  2.5 |           0 | Meatpacking/West Village West | SoHo                      | 02:51:32 | 00:15:05.956522 |       1
        2 | 2022-01-02 16:00:06  | 2022-01-03 15:12:12   |               2 |          1.81 |          1 | N                  |          141 |          239 |            1 |         9.5 |     0 |     0.5 |       2.56 |            0 |                   0.3 |        15.36 |                  2.5 |           0 | Lenox Hill West               | Upper West Side South     | 23:12:06 | 00:35:40.814815 |       1
        1 | 2022-01-01 01:19:33  | 2022-01-01 01:32:28   |               1 |           2.3 |          1 | N                  |          261 |          113 |            1 |          11 |     3 |     0.5 |       2.95 |            0 |                   0.3 |        17.75 |                  2.5 |           0 | World Trade Center            | Greenwich Village North   | 00:12:55 | 02:00:52.923077 |       1
        2 | 2022-01-01 02:36:10  | 2022-01-02 02:33:56   |               1 |          4.86 |          1 | N                  |           48 |            7 |            1 |          18 |   0.5 |     0.5 |          0 |            0 |                   0.3 |         21.8 |                  2.5 |           0 | Clinton East                  | Astoria                   | 23:57:46 | 01:06:02.709677 |       1
        2 | 2022-01-01 01:48:39  | 2022-01-02 01:46:16   |               1 |          1.61 |          1 | N                  |           41 |           42 |            2 |           8 |   0.5 |     0.5 |          0 |            0 |                   0.3 |          9.3 |                    0 |           0 | Central Harlem                | Central Harlem North      | 23:57:37 | 00:43:25.923077 |       1
        2 | 2022-01-01 20:15:44  | 2022-01-02 00:00:00   |               1 |          1.91 |          1 | N                  |           90 |          163 |            1 |         9.5 |   0.5 |     0.5 |       3.32 |            0 |                   0.3 |        16.62 |                  2.5 |           0 | Flatiron                      | Midtown North             | 03:44:16 | 00:20:49.25     |       1
        2 | 2022-01-02 12:24:19  | 2022-01-03 12:20:32   |               1 |          0.86 |          1 | N                  |           68 |          246 |            1 |         4.5 |     0 |     0.5 |       1.17 |            0 |                   0.3 |         8.97 |                  2.5 |           0 | East Chelsea                  | West Chelsea/Hudson Yards | 23:56:13 | 00:20:12.412371 |       1
(10 rows)

```

# 3

```sql
-- Question 3

-- From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups? For example if the latest pickup time is 2020-01-01 12:00:00, then the query should return the top 3 busiest zones from 2020-01-01 11:00:00 to 2020-01-01 12:00:00.

-- HINT: You can use dynamic filter pattern to create a filter condition based on the latest pickup time.

-- NOTE: For this question 17 hours was picked to ensure we have enough data to work with.

-- Options:

--     Clinton East, Upper East Side North, Penn Station
--     LaGuardia Airport, Lincoln Square East, JFK Airport
--     Midtown Center, Upper East Side South, Upper East Side North
--     LaGuardia Airport, Midtown Center, Upper East Side North

-- Group TopN pattern: https://docs.risingwave.com/docs/current/sql-pattern-topn/

DROP MATERIALIZED VIEW IF EXISTS busy_pu_zones;

CREATE MATERIALIZED VIEW busy_pu_zones AS

WITH t AS (
        SELECT MAX(tpep_pickup_datetime) AS latest_pickup_time
        FROM trip_data
    )

SELECT
    taxi_zone.Zone AS pickup_zone_zone,
    count(*) AS last_17_h_pickup_cnt
       FROM t,
           trip_data
       JOIN taxi_zone
           ON trip_data.PULocationID = taxi_zone.location_id
       WHERE
           trip_data.tpep_pickup_datetime > (t.latest_pickup_time - INTERVAL '17' HOUR)
       GROUP BY
           taxi_zone.Zone
       ;
```

```bash
dev=> select * from busy_pu_zones order by 2 desc;
        pickup_zone_zone        | last_17_h_pickup_cnt 
--------------------------------+----------------------
 LaGuardia Airport              |                   19
 Lincoln Square East            |                   17
 JFK Airport                    |                   17
 Penn Station/Madison Sq West   |                   16
 Upper East Side North          |                   13
 Times Sq/Theatre District      |                   12
 East Chelsea                   |                   11
 Upper East Side South          |                   10
 Clinton East                   |                    8
 Lenox Hill West                |                    8
```