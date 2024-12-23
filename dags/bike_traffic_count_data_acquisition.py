from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from utils.acquisition_utils import send_alert, insert_into_clean, data_fetch
import logging
import pandas as pd
import re

ENDPOINT = "https://data.smartdublin.ie/dataset/d26ce6c0-2e1c-4b72-8fbd-cb9f9cbbc118/resource/d5c64b9a-27af-411f-81b1-35b3add5a279/download/cycle-counts-1-jan-31-december-2023.xlsx"
TABLE = "bike_traffic_count"
DAG_NAME = "bike_traffic_data_acquisition"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

def data_quality_check(df):
    if df.empty:
        print("Data Quality check - fail")
        raise "Data quality failed"
    
    print("Data Quality check - pass")

def clean_column_names(columns):
    return [
        col if col.startswith('Griffith Avenue') else re.sub(r' \([^)]*\)', '', col)
        for col in columns
    ]

def clean_data(df):

    try:
        df = df.loc[:, ~df.columns.str.startswith(('North Strand Rd', 'Drumcondra Cyclists Outbound'))]
        df.columns = clean_column_names(df.columns)

        df = df.rename(columns={"Time": "time_reported"})

        df['acquisition_time'] = pd.to_datetime(df['acquisition_time'])
        df['time_reported'] = df['time_reported'].astype('datetime64[us]').dt.tz_localize('UTC')

        return df
    
    except Exception as e:
        logging.error(f"Error cleaning data: {e}")
        raise e


def data_acquisition():
    url = ENDPOINT
    try:
        df = data_fetch(url, file_format='xls')
        df = clean_data(df)
        data_quality_check(df)
        insert_into_clean(df, TABLE)

    except Exception as e:
        logging.error(f"Error in data acquisition: {e}")
        send_alert(DAG_NAME, e)
        raise e

with DAG(DAG_NAME,
         default_args=default_args,
         description='DAG to acquire, clean, and insert Dublin bus passengers data from CSO',
         schedule_interval='@daily',
         start_date=datetime(2024, 1, 1),
         catchup=False) as dag:

    data_acquisition_task = PythonOperator(
        task_id='data_acquisition',
        python_callable=data_acquisition,
        provide_context=True
    )

    data_acquisition_task