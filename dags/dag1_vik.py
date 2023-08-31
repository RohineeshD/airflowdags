from datetime import datetime
from airflow import DAG
from datetime import timedelta, datetime
import requests
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from io import StringIO
import pandas as pd
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'exusia_team',
    'start_date': datetime(2023, 8, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

snowflake_conn_id = 'snowflake_connection'

def read_and_load_data(**kwargs):
    
    # Read CSV file using pandas
    csv_file_url = 'https://raw.githubusercontent.com/cs109/2014_data/master/countries.csv'
    response = requests.get(csv_file_url)
    data = response.text
    df = pd.read_csv(StringIO(data))
    print("thedata")
    print(df)
    snowflake_conn_id = 'snowflake_connection'
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_connection')
    schema = 'af_sch'
    table_name = 'stag_vikas'
    connection = snowflake_hook.get_conn()
    snowflake_hook.insert_rows(table_name, df.values.tolist())
    print("Inserting data into staging table")

def check_data_loading():
    
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_connection')
    schema = 'af_sch'
    table_name = 'stag_vikas'
    connection = snowflake_hook.get_conn()
    query = "SELECT COUNT(*) FROM stag_vikas;"
    result = snowflake_hook.get_first(query)
    
    if result[0] > 0:
        print("Data loaded successfully")
        True
    else:
        print("Data is not loaded")
        raise AirflowSkipException("Skipping tasks due to condition not met")

with DAG('dag1_vik', default_args=default_args, schedule_interval=None) as dag:
    
       
    read_and_load_data = PythonOperator(
    task_id='read_and_load_data',
    python_callable=read_and_load_data,
    dag=dag
    )

    check_data_loading = PythonOperator(
    task_id='check_data_loading',
    python_callable=check_data_loading,
    dag=dag,
    )

    trigger_dag2_task = TriggerDagRunOperator(
    task_id='trigger_dag2_task',
    trigger_dag_id="dag2_vik", 
    python_callable=check_data_loading,  # Condition to trigger DAG2
    dag=dag,
    )

# Setting up task dependencies 
read_and_load_data  >>  check_data_loading  >>  trigger_dag2_task
