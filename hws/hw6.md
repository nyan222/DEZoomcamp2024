## 1
```bash
rpk version
v23.2.26 (rev 328d83a06e)
```

## 2
```bash
% rpk topic create test-topic --partitions 4 --replicas 2
TOPIC       STATUS
test-topic  INVALID_REPLICATION_FACTOR: replication factor must be odd
% rpk topic create test-topic --partitions 4 --replicas 3
TOPIC       STATUS
test-topic  INVALID_REPLICATION_FACTOR: Replication factor is below 1 or larger than the number of available brokers.
% rpk topic create test-topic --partitions 4 --replicas 1
TOPIC       STATUS
test-topic  OK
```

## 3
True

## 4
```python
t0 = time.time()

topic_name = 'test-topic'

for i in range(10):
    message = {'number': i}
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")
    time.sleep(0.05)
t2 = time.time()
print(f'before flush it took {(t2 - t0):.2f} seconds')
producer.flush()

t1 = time.time()
print(f'after flush it took {(t1 - t0):.2f} seconds')
```
```bash
Sent: {'number': 0}
Sent: {'number': 1}
Sent: {'number': 2}
Sent: {'number': 3}
Sent: {'number': 4}
Sent: {'number': 5}
Sent: {'number': 6}
Sent: {'number': 7}
Sent: {'number': 8}
Sent: {'number': 9}
before flush it took 0.54 seconds
after flush it took 0.54 seconds
```

## 5
```bash
rpk topic create green-trips --partitions 4 --replicas 1
```
```python
topic_name = 'green-trips'
t0 = time.time()
for row in df_green.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}

    # Send the data to the Kafka topic
    producer.send(topic=topic_name, value=row_dict)
t2 = time.time()
print(f'before flush it took {(t2 - t0):.2f} seconds')
producer.flush()
```
```bash
before flush it took 24.78 seconds
```

## 6

With the function from hw instructions
```python
def peek(mini_batch, batch_id):
    first_row = mini_batch.take(1)

    if first_row:
        print(first_row[0])

query = green_stream.writeStream.foreachBatch(peek).start()
```
I've got
```bash
Row(lpep_pickup_datetime='2019-10-01 00:09:31', lpep_dropoff_datetime='2019-10-01 00:20:41', PULocationID=41, DOLocationID=74, passenger_count=1.0, trip_distance=2.03, tip_amount=2.16)
```
And with debug function
```python
from pyspark.sql import functions as F

def debug_and_process(batch_df, batch_id):
    print(f"Batch ID: {batch_id}")
    batch_df.select(F.col("value").cast('STRING')).show(truncate=False)
    parsed_batch = batch_df.select(F.from_json(F.col("value").cast('STRING'), schema).alias("data")) \
    .select("data.*")
   

    parsed_batch.show(truncate=False)

query = green_stream \
    .writeStream \
    .foreachBatch(debug_and_process) \
    .start()
```
I've got
```bash
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|value                                                                                                                                                                                                       |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|{"lpep_pickup_datetime": "2019-10-01 00:09:31", "lpep_dropoff_datetime": "2019-10-01 00:20:41", "PULocationID": 41, "DOLocationID": 74, "passenger_count": 1.0, "trip_distance": 2.03, "tip_amount": 2.16}  |
|{"lpep_pickup_datetime": "2019-10-01 00:30:36", "lpep_dropoff_datetime": "2019-10-01 00:34:30", "PULocationID": 41, "DOLocationID": 42, "passenger_count": 1.0, "trip_distance": 0.73, "tip_amount": 1.26}  |
|{"lpep_pickup_datetime": "2019-10-01 00:42:27", "lpep_dropoff_datetime": "2019-10-01 01:04:30", "PULocationID": 260, "DOLocationID": 92, "passenger_count": 1.0, "trip_distance": 4.21, "tip_amount": 0.0}  |
|{"lpep_pickup_datetime": "2019-10-01 00:22:41", "lpep_dropoff_datetime": "2019-10-01 00:31:53", "PULocationID": 33, "DOLocationID": 80, "passenger_count": 1.0, "trip_distance": 4.46, "tip_amount": 0.0}   |
|{"lpep_pickup_datetime": "2019-10-01 00:38:00", "lpep_dropoff_datetime": "2019-10-01 00:45:50", "PULocationID": 256, "DOLocationID": 37, "passenger_count": 1.0, "trip_distance": 1.81, "tip_amount": 0.0}  |
|{"lpep_pickup_datetime": "2019-10-01 00:52:36", "lpep_dropoff_datetime": "2019-10-01 00:52:42", "PULocationID": 92, "DOLocationID": 92, "passenger_count": 1.0, "trip_distance": 0.0, "tip_amount": 0.0}    |
|{"lpep_pickup_datetime": "2019-10-01 00:57:00", "lpep_dropoff_datetime": "2019-10-01 00:59:27", "PULocationID": 7, "DOLocationID": 7, "passenger_count": 1.0, "trip_distance": 0.53, "tip_amount": 0.0}     |
|{"lpep_pickup_datetime": "2019-10-01 00:54:17", "lpep_dropoff_datetime": "2019-10-01 01:17:54", "PULocationID": 25, "DOLocationID": 75, "passenger_count": 1.0, "trip_distance": 10.3, "tip_amount": 6.91}  |
|{"lpep_pickup_datetime": "2019-10-01 00:22:21", "lpep_dropoff_datetime": "2019-10-01 00:34:36", "PULocationID": 244, "DOLocationID": 239, "passenger_count": 1.0, "trip_distance": 5.1, "tip_amount": 2.0}  |
|{"lpep_pickup_datetime": "2019-10-01 00:42:36", "lpep_dropoff_datetime": "2019-10-01 00:43:02", "PULocationID": 74, "DOLocationID": 74, "passenger_count": 1.0, "trip_distance": 0.0, "tip_amount": 35.0}   |
|{"lpep_pickup_datetime": "2019-10-01 00:29:21", "lpep_dropoff_datetime": "2019-10-01 00:36:35", "PULocationID": 82, "DOLocationID": 157, "passenger_count": 1.0, "trip_distance": 1.45, "tip_amount": 2.49} |
|{"lpep_pickup_datetime": "2019-10-01 00:49:41", "lpep_dropoff_datetime": "2019-10-01 01:14:27", "PULocationID": 129, "DOLocationID": 130, "passenger_count": 1.0, "trip_distance": 9.94, "tip_amount": 0.0} |
|{"lpep_pickup_datetime": "2019-10-01 00:44:50", "lpep_dropoff_datetime": "2019-10-01 00:49:05", "PULocationID": 75, "DOLocationID": 75, "passenger_count": 1.0, "trip_distance": 0.93, "tip_amount": 1.26}  |
|{"lpep_pickup_datetime": "2019-10-01 00:04:16", "lpep_dropoff_datetime": "2019-10-01 00:09:30", "PULocationID": 74, "DOLocationID": 168, "passenger_count": 1.0, "trip_distance": 1.31, "tip_amount": 0.0}  |
|{"lpep_pickup_datetime": "2019-10-01 00:16:55", "lpep_dropoff_datetime": "2019-10-01 00:29:49", "PULocationID": 92, "DOLocationID": 192, "passenger_count": 1.0, "trip_distance": 2.61, "tip_amount": 3.84} |
|{"lpep_pickup_datetime": "2019-10-01 00:27:18", "lpep_dropoff_datetime": "2019-10-01 00:34:21", "PULocationID": 74, "DOLocationID": 168, "passenger_count": 2.0, "trip_distance": 1.25, "tip_amount": 0.0}  |
|{"lpep_pickup_datetime": "2019-10-01 00:05:15", "lpep_dropoff_datetime": "2019-10-01 00:14:52", "PULocationID": 181, "DOLocationID": 181, "passenger_count": 1.0, "trip_distance": 1.93, "tip_amount": 2.58}|
|{"lpep_pickup_datetime": "2019-10-01 00:10:15", "lpep_dropoff_datetime": "2019-10-01 00:23:35", "PULocationID": 80, "DOLocationID": 61, "passenger_count": 1.0, "trip_distance": 3.66, "tip_amount": 2.96}  |
|{"lpep_pickup_datetime": "2019-10-01 00:34:46", "lpep_dropoff_datetime": "2019-10-01 00:44:34", "PULocationID": 260, "DOLocationID": 7, "passenger_count": 1.0, "trip_distance": 1.78, "tip_amount": 0.0}   |
|{"lpep_pickup_datetime": "2019-10-01 00:01:53", "lpep_dropoff_datetime": "2019-10-01 00:14:24", "PULocationID": 130, "DOLocationID": 205, "passenger_count": 1.0, "trip_distance": 3.44, "tip_amount": 4.29}|
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
only showing top 20 rows

+--------------------+---------------------+------------+------------+---------------+-------------+----------+
|lpep_pickup_datetime|lpep_dropoff_datetime|PULocationID|DOLocationID|passenger_count|trip_distance|tip_amount|
+--------------------+---------------------+------------+------------+---------------+-------------+----------+
|2019-10-01 00:09:31 |2019-10-01 00:20:41  |41          |74          |1.0            |2.03         |2.16      |
|2019-10-01 00:30:36 |2019-10-01 00:34:30  |41          |42          |1.0            |0.73         |1.26      |
|2019-10-01 00:42:27 |2019-10-01 01:04:30  |260         |92          |1.0            |4.21         |0.0       |
|2019-10-01 00:22:41 |2019-10-01 00:31:53  |33          |80          |1.0            |4.46         |0.0       |
|2019-10-01 00:38:00 |2019-10-01 00:45:50  |256         |37          |1.0            |1.81         |0.0       |
|2019-10-01 00:52:36 |2019-10-01 00:52:42  |92          |92          |1.0            |0.0          |0.0       |
|2019-10-01 00:57:00 |2019-10-01 00:59:27  |7           |7           |1.0            |0.53         |0.0       |
|2019-10-01 00:54:17 |2019-10-01 01:17:54  |25          |75          |1.0            |10.3         |6.91      |
|2019-10-01 00:22:21 |2019-10-01 00:34:36  |244         |239         |1.0            |5.1          |2.0       |
|2019-10-01 00:42:36 |2019-10-01 00:43:02  |74          |74          |1.0            |0.0          |35.0      |
|2019-10-01 00:29:21 |2019-10-01 00:36:35  |82          |157         |1.0            |1.45         |2.49      |
|2019-10-01 00:49:41 |2019-10-01 01:14:27  |129         |130         |1.0            |9.94         |0.0       |
|2019-10-01 00:44:50 |2019-10-01 00:49:05  |75          |75          |1.0            |0.93         |1.26      |
|2019-10-01 00:04:16 |2019-10-01 00:09:30  |74          |168         |1.0            |1.31         |0.0       |
|2019-10-01 00:16:55 |2019-10-01 00:29:49  |92          |192         |1.0            |2.61         |3.84      |
|2019-10-01 00:27:18 |2019-10-01 00:34:21  |74          |168         |2.0            |1.25         |0.0       |
|2019-10-01 00:05:15 |2019-10-01 00:14:52  |181         |181         |1.0            |1.93         |2.58      |
|2019-10-01 00:10:15 |2019-10-01 00:23:35  |80          |61          |1.0            |3.66         |2.96      |
|2019-10-01 00:34:46 |2019-10-01 00:44:34  |260         |7           |1.0            |1.78         |0.0       |
|2019-10-01 00:01:53 |2019-10-01 00:14:24  |130         |205         |1.0            |3.44         |4.29      |
+--------------------+---------------------+------------+------------+---------------+-------------+----------+
only showing top 20 rows
```

PS Thanks from all my heart to Taras Sh from the Slack of Data Talking Club for debugging my stream problem

## 7

```python
from pyspark.sql import functions as F

green_stream = green_stream \
  .select(F.from_json(F.col("value").cast('STRING'), schema).alias("data")) \
  .select("data.*")

    # Add a column "timestamp" using the current_timestamp function
    # Group by:
    #     5 minutes window based on the timestamp column (F.window(col("timestamp"), "5 minutes"))
    #     "DOLocationID"
    # Order by count

popular_destinations = green_stream \
    .withColumn("timestamp", F.current_timestamp()) \
    .groupBy(
        F.window("timestamp", "5 minutes"),
        "DOLocationID"
    ) \
    .count() \
    .orderBy(F.col("count").desc())
```

```bash
+------------------------------------------+------------+-----+
|window                                    |DOLocationID|count|
+------------------------------------------+------------+-----+
|{2024-03-14 21:10:00, 2024-03-14 21:15:00}|74          |17741|
|{2024-03-14 21:10:00, 2024-03-14 21:15:00}|42          |15942|
|{2024-03-14 21:10:00, 2024-03-14 21:15:00}|41          |14061|
|{2024-03-14 21:10:00, 2024-03-14 21:15:00}|75          |12840|
|{2024-03-14 21:10:00, 2024-03-14 21:15:00}|129         |11930|
|{2024-03-14 21:10:00, 2024-03-14 21:15:00}|7           |11533|
|{2024-03-14 21:10:00, 2024-03-14 21:15:00}|166         |10845|
|{2024-03-14 21:10:00, 2024-03-14 21:15:00}|236         |7913 |
|{2024-03-14 21:10:00, 2024-03-14 21:15:00}|223         |7542 |
|{2024-03-14 21:10:00, 2024-03-14 21:15:00}|238         |7318 |
|{2024-03-14 21:10:00, 2024-03-14 21:15:00}|82          |7292 |
|{2024-03-14 21:10:00, 2024-03-14 21:15:00}|181         |7282 |
|{2024-03-14 21:10:00, 2024-03-14 21:15:00}|95          |7244 |
|{2024-03-14 21:10:00, 2024-03-14 21:15:00}|244         |6733 |
|{2024-03-14 21:10:00, 2024-03-14 21:15:00}|61          |6606 |
|{2024-03-14 21:10:00, 2024-03-14 21:15:00}|116         |6339 |
|{2024-03-14 21:10:00, 2024-03-14 21:15:00}|138         |6144 |
|{2024-03-14 21:10:00, 2024-03-14 21:15:00}|97          |6050 |
|{2024-03-14 21:10:00, 2024-03-14 21:15:00}|49          |5221 |
|{2024-03-14 21:10:00, 2024-03-14 21:15:00}|151         |5153 |
+------------------------------------------+------------+-----+
only showing top 20 rows
```


