##DATA LOADER load_green_taxi

```python
import io
import pandas as pd
import requests
import glob
import os

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
    
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    #url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
    
    taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'RatecodeID':pd.Int64Dtype(),
                    'store_and_fwd_flag':str,
                    'PULocationID':pd.Int64Dtype(),
                    'DOLocationID':pd.Int64Dtype(),
                    'payment_type': pd.Int64Dtype(),
                    'fare_amount': float,
                    'extra':float,
                    'mta_tax':float,
                    'tip_amount':float,
                    'tolls_amount':float,
                    'improvement_surcharge':float,
                    'total_amount':float,
                    'congestion_surcharge':float,
                    'trip_type': pd.Int64Dtype(),
                    'ehail_fee': float
                }

    # native date parsing 
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    lst = []

    for i in range(10,13):
        url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{0}.csv.gz".format(i)
        print(url)
        df = pd.read_csv(
        url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates
        )
        
        lst.append(df)

    dfr = pd.concat(lst, axis=0, ignore_index=True)
    
    return dfr


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
```




##TRANSFORMER transform_green_taxi

```python
import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

    print(f"Preprocessing rows with zero passengers: { data[['passenger_count']].isin([0]).sum() }")
    print(f"Preprocessing rows with zero trip_distance: { data[['trip_distance']].isin([0]).sum() }")
    print(f"Preprocessing rows with null trip_distance: { data[['trip_distance']].isna().sum() }")
    
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    pattern = re.compile("([A-Z]+[a-z]+)([A-Z]+)")  # Replace "your_pattern_here" with your regular expression pattern
    matching_columns = [col for col in data.columns if pattern.match(col)]
    num_matching_columns = len(matching_columns)
    print(f"Fields to change into snakecase: {num_matching_columns}")

    data.columns = (data.columns
                .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                .str.lower()
             )

    return data[(data['passenger_count']>0) & (data['trip_distance']>0)]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero trip distance'
    assert 'vendor_id' in output, 'There is no column "vendor_id"'
```




##DATA EXPORTER taxi_data_to_postgres

```python
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    schema_name = 'mage'  # Specify the name of the schema to export data to
    table_name = 'green_taxi'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'dev'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            if_exists='replace',  # Specify resolution policy if table name already exists
        )

```




##DATA LOADER check_postgres

```sql
select distinct vendor_id from mage.green_taxi;
```




##DATA EXPORTER taxi_data_to_gcs_partitioned_parquet

```python
import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/cfk.json"

bucket_name = 'ny_taxi_222'
project_id = 'coral-firefly-411510'
table_name = 'green_taxi_data'
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(

        table,
        root_path = root_path,
        partition_cols = ['lpep_pickup_date'],
        filesystem = gcs
    )
    
```
<img width="1016" alt="bckt" src="https://github.com/nyan222/DEZoomcamp2024/assets/47917537/c02a7734-83c0-48ad-bdea-9d831f67f719">



##SCHEDULE

<img width="653" alt="schd" src="https://github.com/nyan222/DEZoomcamp2024/assets/47917537/e6dcdb43-7881-4ea9-bea9-6dc80e58917c">


