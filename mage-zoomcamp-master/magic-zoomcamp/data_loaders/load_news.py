from google.cloud import storage
import requests
import os
import re
import datetime
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import io

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/cfk.json"

def get_max_blob_name(bucket_name):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)
    
    max_blob_name = max((blob.name for blob in blobs), default=None)
    return max_blob_name

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


bucket_name = 'roman_empire_zoomcamp'
BUCKET = os.environ.get("GCP_GCS_BUCKET", "roman_empire_zoomcamp")
init_url = 'https://huggingface.co/datasets/PleIAs/US-PD-Newspapers/resolve/refs%2Fconvert%2Fparquet/default/train'

@data_loader
def load_data(*args, **kwargs):
    fn = get_max_blob_name(bucket_name)   
    print(fn)
    if fn is not None:
        n = re.search(r'\d+', fn).group()
        print(n) 
        n = int(n)
        m = n + 1
    else:
        n = -1
        m = 0
    t = True
    while t:
        num = '000'+str(m)
        num = num[-4:]
        file_name = f"{num}.parquet"
        request_url = f"{init_url}/{file_name}"
        print(str(datetime.datetime.now())+": "+request_url)
        r = requests.head(request_url, allow_redirects=True)
        print(r.status_code)
        if m == n+1 and r.status_code != 200:
            print('nofile') 
            raise Exception("no new files")
        elif m > n+1 and r.status_code != 200:
            print('nomorefile')  
            break
        else:   
            m = m + 1
            df = pd.read_parquet(request_url,engine='pyarrow')

            df.to_parquet(file_name, engine='pyarrow',coerce_timestamps="ms",allow_truncated_timestamps=True)###
            print(f"Parquet: {file_name}")
    
            # upload to gcs 
            upload_to_gcs(BUCKET, f"{file_name}", file_name)
            print(f"GCS: {file_name}")


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
