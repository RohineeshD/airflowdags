# Step 1: Importing Modules
# To initiate the DAG Object
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

# Step 2: Initiating the default_args
default_args = {
        'owner' : 'airflow',
        'start_date' :days_ago(2)
}

'''
# Define the SQL query you want to execute in Snowflake
query = """
SELECT 7000001 FROM emp_data;
"""
'''

my_regular_var = ''

# Step 4: Creating task
# Creating first task
#start = DummyOperator(task_id = 'start', dag = dag)

def print_env_var():
    print(os.environ["AIRFLOW_CTX_DAG_ID"])

def get_var_regular():    
    my_regular_var = Variable.get("b_var")
    print("Variable value: ",my_regular_var)

def load_data():
  print("Variable value:::::::: ",my_regular_var)
  if Variable.get("b_var").upper() == 'TRUE':
    url = r"https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
    response = requests.get(url)
    data = response.text
    df = pd.read_csv(StringIO(data))
    sf_hook = SnowflakeHook(snowflake_conn_id='sf_bhagya')
    conn = sf_hook.get_conn()
    sf_hook.insert_rows('AIRLINES',df.values.tolist())
    conn.close(); 
    print("File uploaded")
  else:
        print("No File loaded")

def get_sf_data5():
    
    sf_hook = SnowflakeHook(snowflake_conn_id='sf_bhagya')
    conn = sf_hook.get_conn()
    cur = conn.cursor();
    data = '';  

    query1 = "SELECT * FROM AIRLINES WHERE AVAIL_SEAT_KM_PER_WEEK <= 698012498 LIMIT 5"
    data = cur.execute(query1)
    print("Printing 5 records")
    for record in data:
        print(record)
   
    conn.close();

def get_sf_data10():
    
    sf_hook = SnowflakeHook(snowflake_conn_id='sf_bhagya')
    conn = sf_hook.get_conn()
    cur = conn.cursor();
    data = '';  

   
    query1 = "SELECT * FROM AIRLINES WHERE AVAIL_SEAT_KM_PER_WEEK > 698012498 LIMIT 10"
    data = cur.execute(query1)
    print("Printing 10 records")
    for record in data:
        print(record)
        
    conn.close();


def print_query(ti, **kwargs):
    query = ti.xcom_pull(task_ids='execute_snowflake_query')
    print(query)

def print_processed():
    logging.info("Processed")

# Step 3: Creating DAG Object
dag = DAG(dag_id='bhagya_dag',
        default_args=default_args,
        schedule_interval='@once', 
        catchup=False
    )

task_print_context = PythonOperator(
    task_id="print_env",
    python_callable=get_var_regular,
    dag=dag
)

task_load_data = PythonOperator(
    task_id="Load_data_to_Snowflake",
    python_callable=load_data,
    dag=dag
)

task_get_sf_data5 = PythonOperator(
    task_id="Get_Data_From_Snowflake",
    python_callable=get_sf_data5,
    dag=dag
)

task_get_sf_data10 = PythonOperator(
    task_id="Get_Data_From_Snowflake",
    python_callable=get_sf_data10,
    dag=dag
)

'''
# Create a SnowflakeOperator task
task_snowflake_task = SnowflakeOperator(
    task_id='execute_snowflake_query',
    sql=query,
    snowflake_conn_id='sf_bhagya',  # Set this to your Snowflake connection ID
    autocommit=True,  # Set autocommit to True if needed
    dag=dag
)
'''

# Creating second task 
#end = DummyOperator(task_id = 'end', dag = dag)
task_print_processed_end = PythonOperator(
    task_id="print_process",
    python_callable=print_processed,
    dag=dag
)

 # Step 5: Setting up dependencies 
task_print_context >> task_load_data >> task_get_sf_data5,task_get_sf_data10  >> task_print_processed_end
