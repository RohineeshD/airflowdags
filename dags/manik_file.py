# Step 1: Importing Modules
# To initiate the DAG Object
from airflow import DAG
# Importing datetime and timedelta modules for scheduling the DAGs
from datetime import timedelta, datetime
# Importing operators 
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
import pandas as pd 

# Step 2: Initiating the default_args
default_args = {
        'owner' : 'airflow',
        'start_date' : datetime(2023, 8, 25),

}

# Step 3: Creating DAG Object
dag = DAG(dag_id='manik_dag',
        default_args=default_args,
        schedule_interval='@once', 
        catchup=False
    )

def check_env_variable(**kwargs):
    if True:  # Replace with your logic to check the environment variable
        return True
    return False

check_env_task = PythonOperator(
    task_id='check_env_variable',
    python_callable=check_env_variable,
    dag=dag,
)
url = 'https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv'

load_data_task = HttpSensor(
    task_id='load_data_from_url',
    http_conn_id='http_default',  # Configure an HTTP connection in Airflow
    endpoint=url,
    request_params={},  # Additional request parameters if needed
    response_check=lambda response: True if response.status_code == 200 else False,
    poke_interval=5,
    timeout=20,
    mode='poke',
    dag=dag,
)

def filter_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='load_data_from_url')
    filtered_data = data[data['avail_seat_km_per_week'] > 698012498]
    ti.xcom_push(key='filtered_data', value=filtered_data)

filter_data_task = PythonOperator(
    task_id='filter_data',
    python_callable=filter_data,
    provide_context=True,
    dag=dag,
)

def print_records(**kwargs):
    ti = kwargs['ti']
    filtered_data = ti.xcom_pull(task_ids='filter_data', key='filtered_data')
    records_to_print = 10 if filtered_data.shape[0] > 10 else 5
    print(filtered_data.head(records_to_print))

print_records_task = PythonOperator(
    task_id='print_records',
    python_callable=print_records,
    provide_context=True,
    dag=dag,
)


def print_completion(**kwargs):
    print("Process completed!")

completion_task = PythonOperator(
    task_id='print_completion',
    python_callable=print_completion,
    dag=dag,
)

# Define task dependencies
check_env_task  >> filter_data_task >> print_records_task >> completion_task