

from google.cloud import bigquery
from pathlib import Path
# from airflow.utils import timezone
import pandas as pd
import os
import io
import zipfile
from google.cloud import  storage, bigquery
import warnings
warnings.filterwarnings("ignore")

BUCKET_NAME = 'shc-test-bucket'
SOURCE_DATASET = 'tonygordonjr/spotify-dataset-2023'
file_list = ['spotify_tracks_data_2023.csv', 'spotify-albums_data_2023.csv']
# BQ_TABLE = 'your_project.your_dataset.tracks'
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)
bq_client = bigquery.Client('bcs-edf-development')

albums_table_id='bcs-edf-development.kaggle_dataset.albums'
tracks_table_id='bcs-edf-development.kaggle_dataset.tracks'


def download_and_upload_to_gcs(file_list):

    for file in file_list:
        
        # Set environment and download files
        os.environ['KAGGLE_CONFIG_DIR'] = "/home/samridhi_bisht_ext/airflow"

        # Run the Kaggle API using os.system
        os.system(f"kaggle datasets download -d tonygordonjr/spotify-dataset-2023 --file {file} -p /tmp")

        zip_path=Path("/tmp")/file

        extract_dir=Path("/tmp/unzipped_data")
        extract_dir.mkdir(exist_ok=True)

        if zipfile.is_zipfile(zip_path):
            print("Unzipping...")
            with zipfile.ZipFile(zip_path, "r") as f:
                f.extractall(extract_dir)
            print("Unzip compleletd...")

        else:
            print("File is not zipped!")
        
        BLOB_NAME = f'spotify/{file}'

        csv_path=f"{extract_dir}/{file}"

        # Upload to GCS
        blob = bucket.blob(BLOB_NAME)
        blob.upload_from_filename(csv_path)

 
def smart_infer_column(col):
    # Treat empty strings as nulls
    col = col.replace('', pd.NA)
 
    # Get non-null values
    non_null = col.dropna()
 
    # If all values are null, make string
    if non_null.empty:
        return col.astype(str)

    #If column contains True/False value, convert to string
    if non_null.map(type).eq(bool).all():
        return col.astype(bool)

    # Try numeric
    numeric_converted = pd.to_numeric(non_null, errors='coerce')
    if numeric_converted.notna().all():
        if (numeric_converted % 1 != 0).any():
            # At least one float-like value
            return pd.to_numeric(col, errors='coerce').fillna(0.0).astype(float)
        else:
            return pd.to_numeric(col, errors='coerce').fillna(0).astype(int)

    # Try datetime
    datetime_converted = pd.to_datetime(non_null, errors='coerce')
    if datetime_converted.notna().all():
        return pd.to_datetime(col, errors='coerce')  # nulls become NaT

 
    # Else string
    return col.astype(str)


def define_schema(df_null_drop):
            
    # Apply to each column
    for col in df_null_drop.columns:
        df_null_drop[col] = smart_infer_column(df_null_drop[col])
    
    return df_null_drop


def load_to_bigquery(df, table_id):
    # Load to BigQuery
    print(f"Loading data into {table_id} ...")
    job = bq_client.load_table_from_dataframe(
        df,
        destination=table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"
        )
    )
    job.result()
    if job.errors:
        print("Upload failed with errors", job.errors)


def clean_data(df):

    print(f"CLEANING DATA  ....")

    print(f"Original Column Count\n {len(df.columns)}")

    # Drop Null columns
    df_null_drop=df.dropna(how='all', axis=1)
    print(f"New Column Count\n {len(df_null_drop.columns)}")
    
    # Set proper schema and handle null values
    updated_schema_df=define_schema(df_null_drop)

    return updated_schema_df


####    STEP 1  Download files from Kaggle Dataset
download_and_upload_to_gcs(file_list)

####    STEP 2  Read the files as dataframe from GCS Bucket
albums_df=pd.read_csv(f"gs://{BUCKET_NAME}/spotify/spotify-albums_data_2023.csv", encoding='utf-8')
tracks_df=pd.read_csv(f"gs://{BUCKET_NAME}/spotify/spotify_tracks_data_2023.csv", encoding='utf-8')

####    STEP 3  Clean the files, and define their schemas automatically
clean_albums_df=clean_data(albums_df)
updated_tracks_df=define_schema(tracks_df)

####    STEP 4  Perform Transformations
clean_albums_df['radio_mix']=clean_albums_df['duration_sec'].apply(lambda x: 'True' if x <=180 else 'False')
tracks_filtered_df=updated_tracks_df[(updated_tracks_df['explicit']==False) & (updated_tracks_df['track_popularity']>50) ]

####    STEP 5  Load data to Bigquery Tables
clean_albums_df=clean_albums_df.reset_index(drop=True)
load_to_bigquery(clean_albums_df,albums_table_id)
tracks_filtered_df=tracks_filtered_df.reset_index(drop=True)
load_to_bigquery(tracks_filtered_df,tracks_table_id)

####    STEP 6  Perform analysis

sql1 =f"""with cte as
(SELECT label, track_name, track_popularity
FROM `{albums_table_id}` a
join `{tracks_table_id}` t
on a.track_id=t.id
order by track_popularity desc
limit 20
)
select label, count(*) as total_tracks
from cte
group by 1
order by total_tracks desc"""

job1 = bq_client.query(sql1)
top_labels = job1.to_dataframe()
print(f"TOP 20 LABELS:\n {top_labels}")

sql2 =f"""SELECT track_name, release_date FROM `{albums_table_id}` a
            join `{tracks_table_id}` t
            on a.track_id=t.id
            where extract(year from release_date)>=2020 and extract(year from release_date)<=2023
            order by track_popularity desc
            limit 25"""
job2 = bq_client.query(sql2)
top_tracks = job2.to_dataframe()
print(f"TOP 25 TRACKS BETWEEN 2020 & 2023:\n {top_tracks}")