import logging
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
from io import StringIO
import pandas as pd
import requests
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import ShortCircuitOperator


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



default_args = dict(
    start_date= datetime(2021, 1, 1),
    owner="airflow",
    retries=1,
)


dag_args = dict(
    dag_id="shyanjali_dag",
    schedule_interval='@once',
    default_args=default_args,
    catchup=False,
)


def check_environment_variable():

    # variable_value = Variable.get('AIRFLOW_LI')
    # return variable_value == "True"
    if Variable.get('AIRFLOW_SS') == 'True':
        return True
    else:
        #stop dag
        return False
    
def fetch_csv_and_upload(**kwargs):
    url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
    response = requests.get(url)
    data = response.text
    df = pd.read_csv(StringIO(data))


    # Upload DataFrame to Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_li')
    # Replace with your Snowflake schema and table name
    schema = 'PUBLIC'
    table_name = 'AIRLINE'
    connection = snowflake_hook.get_conn()
    snowflake_hook.insert_rows(table_name, df.values.tolist())
    connection.close()


def get_data(**kwargs):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_li')
    connection = snowflake_hook.get_conn()
    create_table_query="SELECT * FROM AIRLINE WHERE avail_seat_km_per_week >698012498 LIMIT 10"
    cursor = connection.cursor()
    records = cursor.execute(create_table_query)
    if records:
        print("10 records")
    else:
        create_table_query="SELECT * FROM AIRLINE WHERE avail_seat_km_per_week LIMIT 5"
        cursor = connection.cursor()
        records = cursor.execute(create_table_query)
        print("5 records")

    for record in records:
        print(record)

    cursor.close()
    connection.close()

def print_success(**kwargs):
    logging.info("Process Completed")


with DAG(**dag_args) as dag:
    # first task declaration
    check_env_variable = ShortCircuitOperator(
    task_id='check_env_variable',
    python_callable=check_environment_variable,
    provide_context=True,
    op_kwargs={},
    )

    fetch_and_upload = PythonOperator(
        task_id='fetch_and_upload',
        python_callable=fetch_csv_and_upload,
        provide_context=True,
        op_kwargs={},# This is required to pass context to the function
    )

    get_data = PythonOperator(
        task_id='get_data',
        python_callable=get_data,
        provide_context=True,
        op_kwargs={},# This is required to pass context to the function
    )

    print_success = PythonOperator(
        task_id='print_success',
        python_callable=print_success,
        provide_context=True,
        op_kwargs={},# This is required to pass context to the function
    )

    check_env_variable >> fetch_and_upload >>get_data>>print_success

