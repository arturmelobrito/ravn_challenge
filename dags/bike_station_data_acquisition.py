from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, timezone
from utils.acquisition_utils import send_alert, insert_into_clean
from dateutil.relativedelta import relativedelta
import logging
import pandas as pd
import requests
import time

ENDPOINT = "https://data.smartdublin.ie/dublinbikes-api/bikes/dublin_bikes/historical/stations"
TABLE = "bike_stations"
DAG_NAME = "bike_stations_data_acquisition"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

def data_fetch(url, start_date, acquisition_time, retries=5):
    attempt = 1
    while attempt <= retries:
        try:
            response = requests.get(url, params={"dt_start":start_date})
            df = pd.DataFrame(response.json())
            df['acquisition_time'] = acquisition_time

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

    try:
        df['last_reported'] = df['last_reported'].astype('datetime64[us]').dt.tz_localize('UTC', ambiguous='True')
        df['datetime_sk'] = df['last_reported'].apply(lambda x: int(x.strftime('%Y%m%d%H%M')))

        df = df.rename(columns={'lon':'long'})

        df['last_reported'] = df['last_reported'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['num_bikes_available'] = df['num_bikes_available'].astype(int)
        df['num_docks_available'] = df['num_docks_available'].astype(int)
        df['capacity'] = df['capacity'].astype(int)
        df['station_id'] = df['station_id'].astype(str)
        df['is_installed'] = df['is_installed'].astype(bool)
        df['is_renting'] = df['is_renting'].astype(bool)
        df['is_returning'] = df['is_returning'].astype(bool)
        df['address'] = df['address'].astype(str)
        df['lat'] = df['lat'].astype(float)
        df['long'] = df['long'].astype(float)
        df['acquisition_time'] = pd.to_datetime(df['acquisition_time'])


        df = df[['last_reported', 'num_bikes_available', 'num_docks_available', 'capacity', 'station_id', 
                 'is_installed', 'is_renting', 'is_returning', 'address', 'lat', 'long', 'acquisition_time']]

        return df
  
    except Exception as e:
        logging.error(f"Error cleaning data: {e}")
        raise e


def data_acquisition():
    url = ENDPOINT
    acquisition_time = datetime.now(timezone.utc)
    three_months_ago = (acquisition_time - relativedelta(months=3)).date()
    start_date = three_months_ago
    n_requests = 0
    batch_df = pd.DataFrame()

    try:
        while start_date <= acquisition_time.date():
            df = data_fetch(url, start_date=start_date, acquisition_time=acquisition_time)
            batch_df = pd.concat([batch_df, df])
            n_requests += 1

            if n_requests % 15 == 0:

                overwrite = True if n_requests == 15 else False
                batch_df = clean_data(batch_df)
                insert_into_clean(batch_df, TABLE, overwrite=overwrite)
                batch_df = pd.DataFrame()
                
            start_date += relativedelta(days=1)
        
        if not batch_df.empty:
            batch_df = clean_data(batch_df)
            insert_into_clean(batch_df, TABLE, overwrite=False)


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
