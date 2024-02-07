import io
import pandas as pd
import requests
import glob
import os
import datetime

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
    
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
service = 'yellow'

@data_loader
def load_data_from_api(*args, **kwargs):
    edt = kwargs.get('execution_date')
    print(edt)
    year = edt.year
    print(year)
    month = edt.month
    print(month)
    
    #url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
    
    #lst = []
    #for year in y:
        #for i in range(12):
            
            # sets the month part of the file_name string
    month = '0'+str(month)
    month = month[-2:]

            # csv file_name
    file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
            ##file_name = f"{service}_tripdata_{year}-{month}.parquet"

            # download it using requests via a pandas df
    request_url = f"{init_url}{service}/{file_name}"
            ##request_url = f"{init_url}{file_name}"
    print(request_url)
    df = pd.read_csv(
    request_url, sep=',', compression='gzip'
    )
            # if service == "yellow":
            #     """Fix dtype issues"""
            #     df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
            #     df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

            # if service == "green":
            #     """Fix dtype issues"""
            #     df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
            #     df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
            #     df["trip_type"] = df["trip_type"].astype('Int64')

            # if service == "yellow" or service == "green":
            #     df["VendorID"] = df["VendorID"].astype('Int64')
            #     df["RatecodeID"] = df["RatecodeID"].astype('Int64')
            #     df["PULocationID"] = df["PULocationID"].astype('Int64')
            #     df["DOLocationID"] = df["DOLocationID"].astype('Int64')
            #     df["passenger_count"] = df["passenger_count"].astype('Int64')
            #     df["payment_type"] = df["payment_type"].astype('Int64')

            # if service == "fhv":
            #     """Rename columns"""
            #     df.rename({'dropoff_datetime':'dropOff_datetime'}, axis='columns', inplace=True)
            #     df.rename({'PULocationID':'PUlocationID'}, axis='columns', inplace=True)
            #     df.rename({'DOLocationID':'DOlocationID'}, axis='columns', inplace=True)

            #     """Fix dtype issues"""
            #     df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
            #     df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])

            #     # See https://pandas.pydata.org/docs/user_guide/integer_na.html
            #     df["PUlocationID"] = df["PUlocationID"].astype('Int64')
            #     df["DOlocationID"] = df["DOlocationID"].astype('Int64')

            #lst.append(df)

    #dfr = pd.concat(lst, axis=0, ignore_index=True)
    
    return df
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")


                


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'