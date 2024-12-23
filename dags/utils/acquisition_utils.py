from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timezone
import logging
import pandas as pd
import time

'''
# Usando SnowflakeHook para obter a conexão SQLAlchemy
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn').get_snowpark_session()
        snowpark_session = hook.get_snowpark_session()

        snowpark_session.write_pandas(df=df, table_name = table, auto_create_table = True, overwrite=True)
        # Usar o to_sql() do pandas para inserir os dados no Snowflake
        #df.to_sql(f"{table}", con=engine, schema = 'clean', if_exists=if_exists, index=False)
        logging.info(f"Data successfully inserted into {table}.")
'''
#Observability Function's placeholder
def send_alert(resource, error):
    print(f"Error in resource {resource}: {error}")

def insert_into_clean(df, table, overwrite=True):
    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        snowpark_session = hook.get_snowpark_session()
        snowpark_session.use_schema('clean')

        snowpark_session.write_pandas(df=df, table_name = table, auto_create_table = True, overwrite=overwrite)
        logging.info(f"Data successfully inserted into {table}.")

    except Exception as e:
        logging.error(f"Error inserting data into Snowflake: {e}")
        raise

def data_fetch(url, retries=5, file_format='csv'):
    attempt = 1
    while attempt <= retries:
        try:
            if file_format == 'csv':
                df = pd.read_csv(url)
            elif file_format == 'xls':
                df = pd.read_excel(url)
                
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
