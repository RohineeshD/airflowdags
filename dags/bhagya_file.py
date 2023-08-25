# Step 1: Importing Modules
# To initiate the DAG Object
from airflow import DAG
import os
# Importing datetime and timedelta modules for scheduling the DAGs
from datetime import timedelta, datetime
# Importing operators 
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
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


# Define the SQL query you want to execute in Snowflake
query = """
SELECT 7000001 FROM emp_data;
"""

my_regular_var = 'NONE'

# Step 4: Creating task
# Creating first task
#start = DummyOperator(task_id = 'start', dag = dag)

def print_env_var():
    print(os.environ["AIRFLOW_CTX_DAG_ID"])

def get_var_regular():    
    my_regular_var = Variable.get("b_var", default_var=None)
    print("Variable value: ",my_regular_var)

def load_data():
        original = r"https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
        delimiter = "," 
        total = pd.read_csv(original, sep = delimiter)
        write_pandas(sf_bhagya, total, "AIRLINES")

def print_query(ti, **kwargs):
    query = ti.xcom_pull(task_ids='execute_snowflake_query')
    print(query)

def print_processed():
    print("Processed")

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

# Create a SnowflakeOperator task
task_snowflake_task = SnowflakeOperator(
    task_id='execute_snowflake_query',
    sql=query,
    snowflake_conn_id='sf_bhagya',  # Set this to your Snowflake connection ID
    autocommit=True,  # Set autocommit to True if needed
    dag=dag
)

# Creating second task 
#end = DummyOperator(task_id = 'end', dag = dag)
task_print_processed_end = PythonOperator(
    task_id="print_process",
    python_callable=print_processed,
    dag=dag
)

 # Step 5: Setting up dependencies 
print_context >> task_load_data >> snowflake_task >> print_processed_end
