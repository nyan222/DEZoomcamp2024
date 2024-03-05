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