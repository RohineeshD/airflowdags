from datetime import datetime
from airflow import DAG
from datetime import timedelta, datetime
import requests
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from io import StringIO
import pandas as pd
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


# Define default arguments for the DAG
default_args = {
    'owner': 'exusia_team',
    'start_date': datetime(2023, 8, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

snowflake_conn_id = 'snowflake_connection'

sql_query = """

INSERT INTO main_vikas(COUNTRY, REGION)
SELECT COUNTRY, REGION
FROM stag_vikas;

"""

with DAG('dag2_vik', default_args=default_args, schedule_interval=None) as dag:
    
       
    stag_to_main = SnowflakeOperator(
    task_id='stag_to_main',
    sql=sql_query,
    snowflake_conn_id='snowflake_connection',  # Connection ID configured in Airflow
    autocommit=True,  # Set to True to automatically commit the transaction
    database='airflow_data',
    warehouse='COMPUTE_WH',
    dag=dag,
    )


# Setting up task dependencies 
stag_to_main
