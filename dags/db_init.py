from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

# Definindo a DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='create_snowflake_database_and_schema',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['init']
) as dag:
    
    # Definindo as queries SQL para criar o banco de dados e esquemas
    create_database_sql = """
    CREATE DATABASE IF NOT EXISTS dublin_public_transport;
    """

    create_staging_schema_sql = """
    CREATE SCHEMA IF NOT EXISTS dublin_public_transport.staging;
    """
    
    create_clean_schema_sql = """
    CREATE SCHEMA IF NOT EXISTS dublin_public_transport.clean;
    """
    create_database = SnowflakeOperator(
        task_id='create_database',
        sql=create_database_sql,
        snowflake_conn_id='snowflake_conn',
    )
    
    create_staging_schema = SnowflakeOperator(
        task_id='create_staging_schema',
        sql=create_staging_schema_sql,
        snowflake_conn_id='snowflake_conn',
    )

    create_clean_schema = SnowflakeOperator(
        task_id='create_clean_schema',
        sql=create_clean_schema_sql,
        snowflake_conn_id='snowflake_conn',
    )


    # Definindo a ordem de execução
    create_database >> create_staging_schema >> create_clean_schema 
