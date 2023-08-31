from datetime import datetime
from airflow import DAG
from datetime import timedelta, datetime
import requests
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from io import StringIO
import pandas as pd

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
    # df_json = df.to_json()
    # kwargs['ti'].xcom_push(key='my_dataframe', value=df)
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
    else:
        print("Data not loaded")

# def load_data_task(**kwargs):
#     ti = kwargs['ti']
#     df = ti.xcom_pull(task_ids='produce_dataframe', key='my_dataframe')
#     print(df)
#     # print(df_json)
    
#     # df = pd.read_json(df_json)
#     print(df)
#     snowflake_conn_id = 'snowflake_connection'
#     snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_connection')
#     schema = 'af_sch'
#     table_name = 'stag_vikas'
#     connection = snowflake_hook.get_conn()
#     snowflake_hook.insert_rows(table_name, df.values.tolist())
#     print("Inserting data into staging table")


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

    # load_data_task = PythonOperator(
    # task_id='load_data_task',
    # python_callable=load_data_task,
    # provide_context=True,
    # dag=dag
    # )

# Setting up task dependencies 
read_and_load_data  >>  check_data_loading
