#Importing Modules
from airflow import DAG
from datetime import timedelta, datetime
import requests
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from io import StringIO
from airflow.exceptions import AirflowSkipException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from datetime import datetime
import pandas as pd

# Step 2: Initiating the default_args
default_args = {
        'owner' : 'airflow',
        'start_date' : datetime(2022, 11, 12),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),

}

#
def extract_and_load_data():
        """ Extracting data from url and loading it into snowflake database"""
        url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
        response = requests.get(url)
        data = response.text
        df = pd.read_csv(StringIO(data))
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_connection')
        schema = 'af_sch'
        table_name = 'data'
        connection = snowflake_hook.get_conn()
        snowflake_hook.insert_rows(table_name, df.values.tolist())

def extract_conditional_data():
        """ Printing records according to conditions"""
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_connection')
        schema = 'af_sch'
        table_name = 'data'
        connection = snowflake_hook.get_conn()
        filter_query1="SELECT * FROM data WHERE avail_seat_km_per_week >698012498 LIMIT 10"
        filter_query2="SELECT * FROM data WHERE avail_seat_km_per_week <698012498 LIMIT 5"
        cursor = connection.cursor()
        cursor.execute(filter_query1)
        print("First 10 values are")
        print(cursor.fetchall())
        print("First 5 values are")
        cursor.execute(filter_query2)
        print(cursor.fetchall())

def completion_message():
        """Printing completion message"""
        print("Process completed")

#defined a function env_var_check which will stop execution of all other tasks if condition doesnt matches
def env_var_check():
        """Will run all the tasks if conditions are met , if it does'nt matches condition it will skip all the tasks"""
        if Variable.get('ENV_CHECK_VIKAS')=='True':    
                print("Environment variable is set to True")
                True        
        else:
                print("Environment variable is set to False")
                raise AirflowSkipException("Skipping tasks due to condition not met")

# Instantiated the DAG with the default_args
with DAG('vikas_dag2', default_args=default_args, schedule_interval=None) as dag:
      
        check_condition_task = PythonOperator(
        task_id='check_condition_task',
        python_callable=env_var_check,
        dag=dag
        )
            
        extract_and_load_data = PythonOperator(
        task_id='extract_and_load_data',
        python_callable=extract_and_load_data,
        provide_context=True,
        )

        extract_conditional_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_conditional_data,
        provide_context=True,
        )

        completion_message = PythonOperator(
        task_id='completion_message',
        python_callable=completion_message,
        provide_context=True,
        )

# Setting up task dependencies 
check_condition_task >> extract_and_load_data >> extract_conditional_data >> completion_message

