import io
import os
import requests
import pandas as pd
from google.cloud import storage
import nltk
nltk.download('punkt')
from nltk import bigrams, word_tokenize
from time import time
import datetime

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
        import os
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/path/to/file.json"
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

init_url = 'https://huggingface.co/datasets/PleIAs/US-PD-Newspapers/resolve/refs%2Fconvert%2Fparquet/default/train'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "roman_empire_zoomcamp")


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


def web_to_gcs(i):
    news_list=[]
    n = i+50
    if n > 2618:
        n = 2618
    print("count "+str(i)+"-"+str(n-1))
    while n > i:
        num = '000'+str(i)
        num = num[-4:]
        file_name = f"{num}.parquet"
        request_url = f"{init_url}/{file_name}"
        print(str(datetime.datetime.now())+": "+request_url)

        df = pd.read_parquet(request_url,engine='pyarrow')
        for index, line in df.iterrows():
            # Tokenize the text
            tokens = word_tokenize(line['text'].lower())
            bi_grams = list(bigrams(tokens))
            if ('roman', 'empire') in bi_grams:
                news_list.append([line.id,line.date,line.word_count, line.text])

        i = i+1
    data = pd.DataFrame(news_list, columns=['id','date','word_count', 'text']) 
    
    data['date'] = pd.to_datetime(data['date'])
    data['year'] = data['date'].dt.year
    
    print(data.head(2))
    print(f"columns: {data.dtypes}")
    print(f"rows: {len(data)}")

    file_rec = f"roman_news_{i-1}.parquet"    
    data.to_parquet(file_rec, engine='pyarrow',coerce_timestamps="ms",allow_truncated_timestamps=True)###
    print(f"Parquet: {file_rec}")
    
    # upload to gcs 
    upload_to_gcs(BUCKET, f"{file_rec}", file_rec)
    print(f"GCS: {file_rec}")


web_to_gcs(0)
web_to_gcs(50)
web_to_gcs(100)
web_to_gcs(150)
web_to_gcs(200)
web_to_gcs(250)
web_to_gcs(300)
web_to_gcs(350)
web_to_gcs(400)
web_to_gcs(450)
web_to_gcs(500)
web_to_gcs(550)
web_to_gcs(600)
web_to_gcs(650)
web_to_gcs(700)
web_to_gcs(750)
web_to_gcs(800)
web_to_gcs(850)
web_to_gcs(900)
web_to_gcs(950)
web_to_gcs(1000)
web_to_gcs(1050)
web_to_gcs(1100)
web_to_gcs(1150)
web_to_gcs(1200)
web_to_gcs(1250)
web_to_gcs(1300)
web_to_gcs(1350)
web_to_gcs(1400)
web_to_gcs(1450)
web_to_gcs(1500)
web_to_gcs(1550)
web_to_gcs(1600)
web_to_gcs(1650)
web_to_gcs(1700)
web_to_gcs(1750)
web_to_gcs(1800)
web_to_gcs(1850)
web_to_gcs(1900)
web_to_gcs(1950)
web_to_gcs(2000)
web_to_gcs(2050)
web_to_gcs(2100)
web_to_gcs(2150)
web_to_gcs(2200)
web_to_gcs(2250)
web_to_gcs(2300)
web_to_gcs(2350)
web_to_gcs(2400)
web_to_gcs(2450)
web_to_gcs(2500)
web_to_gcs(2550)
web_to_gcs(2600)
web_to_gcs(2650)

