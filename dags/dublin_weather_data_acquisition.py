from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, timezone
from utils.acquisition_utils import send_alert, insert_into_clean
import logging
import pandas as pd
import zipfile
import requests
import io
import numpy as np
import time

ENDPOINT = "https://cli.fusio.net/cli/climate_data/webdata/dly3923.zip"
TABLE = "dublin_weather"
DAG_NAME = "dublin_weather_data_acquisition"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

def read_csv_from_zip_url(url, csv_file_name = 'dly3923.csv'):
    response = requests.get(url)
    zip_file = io.BytesIO(response.content)

    with zipfile.ZipFile(zip_file, 'r').open(csv_file_name) as csv_file:
            df = pd.read_csv(csv_file, skiprows=13)
    
    return df

def data_fetch(url, retries=5):
    attempt = 1
    while attempt <= retries:
        try:
            df = read_csv_from_zip_url(url)
            df['acquisition_time'] = datetime.now(timezone.utc)

            logging.info(f"Data fetched and loaded into DataFrame successfully. Rows: {len(df)}")
            return df
        
        except Exception as e:
            logging.error(f"Attempt {attempt} failed: {e}")
            
            if attempt < retries:
                wait_time = 2 ** attempt
                logging.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logging.error(f"Max retries reached. Raising exception.")
                raise e

        attempt += 1

def data_quality_check(df):
    if df.empty:
        print("Data Quality check - fail")
        raise "Data quality failed"
    
    print("Data Quality check - pass")

def clean_data(df):
    cols_to_float = ['rain', 'maxt', 'mint', 'gmin']

    try:
        for col in cols_to_float:
            df[col] = df[col].replace([None, '', ' '], np.nan)
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
        df['acquisition_time'] = pd.to_datetime(df['acquisition_time'])
        df['date_sk'] = df['date'].apply(lambda x: int(x.strftime('%Y%m%d')))
        
        df = df.sort_values(by=['date'] + cols_to_float, ascending=[True] + [True] * len(cols_to_float))
        df = df.drop_duplicates(subset='date', keep='last')

        df = df.rename(columns={
            'rain': 'rain_mm',
            'maxt': 'max_temp_celsius',
            'mint': 'min_temp_celsius',
            'gmin': 'grass_min_temp_celsius'
        })

        df = df[['date', 'date_sk', 'rain_mm', 'max_temp_celsius', 'min_temp_celsius', 'grass_min_temp_celsius', 'acquisition_time']]

        return df
  
    except Exception as e:
        logging.error(f"Error cleaning data: {e}")
        raise e


def data_acquisition():
    url = ENDPOINT
    try:
        df = data_fetch(url)
        df = clean_data(df)
        data_quality_check(df)
        insert_into_clean(df, TABLE)

    except Exception as e:
        logging.error(f"Error in data acquisition: {e}")
        send_alert(DAG_NAME, e)
        raise e

with DAG(DAG_NAME,
         default_args=default_args,
         description='DAG to acquire, clean, and insert Dublin weather data from CSO',
         schedule_interval='@daily',
         start_date=datetime(2024, 1, 1),
         catchup=False) as dag:

    data_acquisition_task = PythonOperator(
        task_id='data_acquisition',
        python_callable=data_acquisition,
        provide_context=True
    )

    data_acquisition_task
