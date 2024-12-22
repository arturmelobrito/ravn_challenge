from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timezone
import logging
import pandas as pd
import time

#Observability Function's placeholder
def send_alert(resource, error):
    print(f"Error in resource {resource}: {error}")

def insert_into_staging(df, table):
    try:
        # Usando SnowflakeHook para obter a conexão SQLAlchemy
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        engine = hook.get_sqlalchemy_engine()

        # Usar o to_sql() do pandas para inserir os dados no Snowflake
        df.to_sql(f"{table}", con=engine, schema = 'staging', if_exists='replace', index=False)
        logging.info(f"Data successfully inserted into {table}.")

    except Exception as e:
        logging.error(f"Error inserting data into Snowflake: {e}")
        raise

def data_fetch(url, retries=5, file_format = 'csv'):
    attempt = 1
    while attempt <= retries:
        try:
            df = pd.read_csv(url)
            df['acquisition_time'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

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