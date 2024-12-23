from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from utils.acquisition_utils import send_alert, insert_into_clean, data_fetch
import logging
import pandas as pd

ENDPOINT = "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.PxAPIv1/en/6/TRANOM/TOA14?query=%7B%22query%22:%5B%5D,%22response%22:%7B%22format%22:%22csv%22,%22pivot%22:null,%22codes%22:false%7D%7D"
TABLE = "dublin_bus_passengers"
DAG_NAME = "dublin_bus_data_acquisition"

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
    

def clean_data(df):

    try:
        df = df[(df['Month'] != 'All months')]

        df = df.rename(columns={
            'Year': 'year',
            'Month': 'month',
            'VALUE': 'passengers'
        })

        df = df.dropna(subset=['year', 'month', 'passengers'])

        df['acquisition_time'] = pd.to_datetime(df['acquisition_time'])
        df['month'] = pd.to_datetime(df['month'], format='%B').dt.strftime('%m')
        df['date_sk'] = df['year'].astype(str) + df['month'].astype(str) + '01'

        df['year'] = df['year'].astype(str)
        df['month'] = df['month'].astype(str)
        df['date_sk'] = df['date_sk'].astype(int)
        df['passengers'] = df['passengers'].astype(int)

        df = df[['year', 'month', 'date_sk', 'passengers', 'acquisition_time']]
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
