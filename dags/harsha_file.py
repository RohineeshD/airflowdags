from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.transfers.local_to_snowflake import LocalFilesystemToSnowflakeOperator
from airflow.providers.http.sensors.http_sensor import HttpSensor
from airflow.utils.dates import days_ago
import pandas as pd
import requests
import tempfile
import os

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # Set the start date
    'catchup': False,
    'provide_context': True,
}

dag = DAG(
    'airline_safety_dag_harsha',
    default_args=default_args,
    schedule_interval=None,  # Set to None for manual triggering
    max_active_runs=1,
    concurrency=1,
    tags=['example'],
)

# Task 1: Check environment variable
def check_env_variable(**kwargs):
    env_variable_value = os.environ.get('MY_ENV_VARIABLE')  # Replace with your actual env variable name
    if env_variable_value == 'true':
        return 'load_data_task'
    else:
        return 'task_end'

task_1 = PythonOperator(
    task_id='check_env_variable',
    python_callable=check_env_variable,
    provide_context=True,
    dag=dag,
)

# Task 2: Read data from URL and load into Snowflake
def read_data_and_load_to_snowflake(**kwargs):
    url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
    response = requests.get(url)
    data = response.text
    
    # Save data to a temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
        temp_file.write(data)
        temp_file_path = temp_file.name
    
    # Load data into Snowflake
    snowflake_task = LocalFilesystemToSnowflakeOperator(
        task_id='load_data_into_snowflake',
        schema='exusia_schema',
        table='airflow_tasks',
        filepaths=[temp_file_path],
        snowflake_conn_id='snowflake_conn',  # Specify your Snowflake connection ID
        dag=dag,
    )
    
    snowflake_task.execute(context=kwargs)

task_2 = PythonOperator(
    task_id='load_data_task',
    python_callable=read_data_and_load_to_snowflake,
    provide_context=True,
    dag=dag,
)

# Task 3: Print process completed
task_end = PythonOperator(
    task_id='task_end',
    python_callable=lambda: print('Process completed'),
    dag=dag,
)

# Define task dependencies
task_1 >> [task_2, task_end]


# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from datetime import datetime

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 1, 1),
#     'retries': 1,
# }

# dag = DAG(
#     'harsha_dag',  
#     default_args=default_args,
#     schedule_interval='@once',
#     catchup=False,
# )

# sql_query = """
# SELECT * FROM patients WHERE status = 'Recovered'
# """

# snowflake_task = SnowflakeOperator(
#     task_id='execute_snowflake_query',
#     sql=sql_query,
#     snowflake_conn_id='snowflake_conn',
#     autocommit=True,
#     dag=dag,
# )



# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from datetime import datetime

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 1, 1),
#     'retries': 1,
# }

# dag = DAG(
#     'harsha_dag',  
#     default_args=default_args,
#     schedule_interval='@once',
#     catchup=False,
# )

# sql_query = """
# "SELECT * FROM patients WHERE status = 'Recovered'
# """

# snowflake_task = SnowflakeOperator(
#     task_id='execute_snowflake_query',
#     sql=sql_query,
#     snowflake_conn_id='snowflake_conn',
#     autocommit=True,
#     dag=dag,
# )

# from airflow import DAG
# from datetime import datetime, timedelta
# from airflow.operators.python_operator import PythonOperator

# # Step 2: Initiating the default_args
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2022, 11, 12),
# }

# # Define a Python function to be executed by the PythonOperator
# def print_hello():
#     print("Hello from the PythonOperator!")

# # Step 3: Creating DAG Object
# dag = DAG(
#     dag_id='harsha_dag',
#     default_args=default_args,
#     schedule_interval='@once',  
#     catchup=False,
# )

# # Step 4: Creating task
# # Create a PythonOperator that will run the print_hello function
# task = PythonOperator(
#     task_id='print_hello_task',
#     python_callable=print_hello,
#     dag=dag,
# )
