
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

df =''

def read_data():
    url = r"https://raw.githubusercontent.com/cs109/2014_data/master/countries.csv"
    response = requests.get(url)
    data = response.text
    df = pd.read_csv(StringIO(data))

def load_data(df1):
 
    sf_hook = SnowflakeHook(snowflake_conn_id='sf_bhagya')
    conn = sf_hook.get_conn()
    sf_hook.insert_rows('PLACE_STAGE',df1.values.tolist())
    conn.close(); 
    print("File uploaded");

def get_data():
    
    sf_hook = SnowflakeHook(snowflake_conn_id='sf_bhagya')
    conn = sf_hook.get_conn()
    cur = conn.cursor();
    data = '';  

   
    query1 = "SELECT * FROM PLACE_STAGE"
    data = cur.execute(query1)
           
    conn.close();

    if(data != None):
        print("Data loaded in the stage table successfully")
    else:
        print("No data in the stage table")

with DAG(dag_id='bhagya_dag1',
        default_args=default_args,
        schedule_interval='@once', 
        catchup=False
    ) as dag:

    task1_read_data = PythonOperator(
        task_id="read_data",
        python_callable=read_data
    )

    task2_load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        op_kwargs={'df1':df}
    )

    task3_get_data = PythonOperator(
        task_id="get_data",
        python_callable=get_data
    )

task1_read_data >> task2_load_data >> task3_get_data
