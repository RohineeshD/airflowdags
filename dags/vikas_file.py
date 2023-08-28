# Step 1: Importing Modules
# To initiate the DAG Object
from airflow import DAG
# Importing datetime and timedelta modules for scheduling the DAGs
from datetime import timedelta, datetime
# Importing operators 
import requests
from airflow.operators.dummy_operator import DummyOperator
#Importing vvariable class
from airflow.models import Variable
from io import StringIO
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
import pandas as pd

# Step 2: Initiating the default_args
default_args = {
        'owner' : 'airflow',
        'start_date' : datetime(2022, 11, 12),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),

}

# Step 3: Creating DAG Object
# dag = DAG(dag_id='vikas_dag',
#         default_args=default_args,
#         schedule_interval='@once', 
#         catchup=False
# )
def check_and_extract_data():
    if Variable.get('ENV_CHECK_VIKAS'):
            url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
            response = requests.get(url)
            data = response.text
            df = pd.read_csv(StringIO(data))
    
            snowflake_hook = SnowflakeHook(snowflake_conn_id='snow_sc')
            table_name = 'data'
            snowflake_hook.insert_rows(table_name, df.values.tolist(), df.columns.tolist())
            print(data)
    else:
            pass
            
        
            
    


with DAG('vikas_dag', default_args=default_args, schedule_interval=None) as dag:
    extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=check_and_extract_data,
    provide_context=True,
    )

check_and_extract_data
# Step 4: Creating task
# Creating first task
# start = DummyOperator(task_id = 'start', dag = dag)
# Creating second task 
# end = DummyOperator(task_id = 'end', dag = dag)

 # Step 5: Setting up dependencies 
# start >> end 
