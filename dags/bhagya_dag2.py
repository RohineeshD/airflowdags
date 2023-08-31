
from io import StringIO
from airflow import DAG
import os
import logging
import requests
# Importing datetime and timedelta modules for scheduling the DAGs
from datetime import timedelta, datetime
# Importing operators 
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas


default_args = {
        'owner' : 'airflow',
        'start_date' :days_ago(2)
    }

query = """INSERT INTO PLACE_MAIN SELECT * FROM PLACE_STAGE;"""
result = ''

def sf_data_check():
    
    sf_hook = SnowflakeHook(snowflake_conn_id='sf_bhagya')
    conn = sf_hook.get_conn()
    cur = conn.cursor();
    data = '';  

   
    query1 = "SELECT * FROM PLACE_MAIN"
    data = cur.execute(query1)
           
    conn.close();

    if(data != None):
       result = "Data loaded in the stage table successfully"
    else:
        result = "No data in the stage table"
    print("Data check is done and respective result is passed to next task")
    return result;
    
def print_result(ti):
    print(ti.xcom_pull(task_ids='sf_data_check'));
    

with DAG(dag_id='bhagya_dag2',
        default_args=default_args,
        schedule_interval='@once', 
        catchup=False
    ) as dag:

    task1_snowflake_task = SnowflakeOperator(
    task_id='execute_snowflake_query',
    sql=query,
    snowflake_conn_id='sf_bhagya',  # Set this to your Snowflake connection ID
    autocommit=True
    )

    task2_sf_data_check = PythonOperator(
        task_id="sf_data_check",
        python_callable=sf_data_check
    )

    task3_print_result = PythonOperator(
        task_id="print_result",
        python_callable=print_result
    )

task1_snowflake_task >> task2_sf_data_check >> task3_print_result
