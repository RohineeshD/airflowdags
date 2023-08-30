from airflow import DAG
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from io import StringIO
import pandas as pd
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = dict(
    start_date= datetime(2021, 1, 1),
    owner="airflow",
    retries=1,
)

dag_args = dict(
    dag_id="Shyanjali_dag2",
    schedule_interval='@once',
    default_args=default_args,
    catchup=False,
)



def insert_to_main(**kwargs):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_li')
    connection = snowflake_hook.get_conn()
    create_table_query="INSERT INTO PUBLIC.MAIN_TABLE SELECT * FROM PUBLIC.STAGING_TABLE;"
    cursor = connection.cursor()
    cursor.execute(create_table_query)
    cursor.close()
    connection.close()



with DAG(**dag_args) as dag:
    # first task declaration
    insert_to_main = PythonOperator(
        task_id='insert_to_main',
        python_callable=insert_to_main,
        provide_context=True,
        op_kwargs={},# This is required to pass context to the function
    )


insert_to_main 
