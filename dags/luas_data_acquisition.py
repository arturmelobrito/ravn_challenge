from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from utils.acquisition_utils import send_alert, insert_into_staging, data_fetch
import logging
import pandas as pd

ENDPOINT = "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/TOA11/CSV/1.0/en"
TABLE = "light_rail_luas_passengers"
DAG_NAME = "luas_data_acquisition"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}


def clean_data(df):

    try:
        df = df[(df['Month'] != 'All months') & (df['Statistic Label'] != 'ALL Luas Lines')]

        df = df.rename(columns={
            'Statistic Label': 'operational_line',
            'Year': 'year',
            'Month': 'month',
            'VALUE': 'passengers'
        })

        df['operational_line'] = df['operational_line'].replace({'Red line': 'Red', 'Green Line': 'Green'})

        df['month'] = pd.to_datetime(df['month'], format='%B').dt.strftime('%m')
        df['date_sk'] = df['year'].astype(str) + df['month'].astype(str) + '01'

        df['operational_line'] = df['operational_line'].astype(str)
        df['year'] = df['year'].astype(str)
        df['month'] = df['month'].astype(str)
        df['date_sk'] = df['date_sk'].astype(int)
        df['passengers'] = df['passengers'].astype(int)
        df['acquisition_time'] = pd.to_datetime(df['acquisition_time'])

        df = df[['operational_line', 'year', 'month', 'date_sk', 'passengers', 'acquisition_time']]

    except Exception as e:
        logging.error(f"Error cleaning data: {e}")
        raise e

    return df

def data_acquisition():
    url = ENDPOINT
    try:
        df = data_fetch(url)
        df = clean_data(df)
        insert_into_staging(df, TABLE)

    except Exception as e:
        logging.error(f"Error in data acquisition: {e}")
        send_alert(DAG_NAME, e)
        raise e

with DAG(DAG_NAME,
         default_args=default_args,
         description='DAG to acquire, clean, and insert Lua transport data from CSO',
         schedule_interval='@daily',
         start_date=datetime(2025, 1, 1),
         catchup=False) as dag:

    data_acquisition_task = PythonOperator(
        task_id='data_acquisition',
        python_callable=data_acquisition,
        provide_context=True
    )

    data_acquisition_task
