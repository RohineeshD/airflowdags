from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd
import requests
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 26),
    'retries': 1,
}

dag = DAG(
    'load_data_into_snowflake',
    default_args=default_args,
    schedule_interval=@hourly,  # Set to None for manual triggering
    catchup=False,
)

# Task 1: Check environment variable
def check_env_variable(**kwargs):
    env_variable_value = os.environ.get('harsh_air_env')
    if env_variable_value == 'true':
        return 'fetch_and_load_to_snowflake'
    else:
        return 'task_end'

task_1 = PythonOperator(
    task_id='check_env_variable',
    python_callable=check_env_variable,
    provide_context=True,
    dag=dag,
)

def data_load_to_snowflake():
    url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
    
    response = requests.get(url)
    if response.status_code == 200:
        csv_data = response.text
        
        # Create a DataFrame from the CSV data
        df = pd.read_csv(pd.compat.StringIO(csv_data))
        
        # Use the SnowflakeHook to load data into Snowflake
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')  
        snowflake_conn = snowflake_hook.get_conn()
        cursor = snowflake_conn.cursor()
        
        # Using destination db schema and table
        destination_schema = 'exusia_schema'
        destination_table = 'airflow'
        
        # Upload the DataFrame to Snowflake
        df.to_sql(destination_table, cursor, schema=destination_schema, if_exists='replace')
        
        cursor.close()
        snowflake_conn.commit()
        snowflake_conn.close()
        
        print("Data loaded into Snowflake successfully.")
    else:
        raise Exception(f"Failed to fetch data from URL. Status code: {response.status_code}")

task2 = PythonOperator(
    task_id='fetch_and_load_to_snowflake',
    python_callable=data_load_to_snowflake,
    dag=dag,
)

task_1 >> task2


# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.utils.dates import days_ago
# import pandas as pd
# import requests
# import tempfile
# import os

# default_args = {
#     'owner': 'airflow',
#     'start_date': days_ago(1),  
#     'catchup': False,

# }

# dag = DAG(
#     'harsha_dag',
#     default_args=default_args,
#     schedule_interval=None, 
    
# )

# # Task 1: Check environment variable
# def check_env_variable(**kwargs):
#     env_variable_value = os.environ.get('harsh_air_env')
#     if env_variable_value == 'true':
#         return 'load_data_task'
#     else:
#         return 'task_end'

# task_1 = PythonOperator(
#     task_id='check_env_variable',
#     python_callable=check_env_variable,
#     provide_context=True,
#     dag=dag,
# )

# def load_data_to_snowflake(**kwargs):
#     url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
    
#     response = requests.get(url)
#     if response.status_code == 200:
#         data = response.text
        
#         # Split the data into lines and exclude the header
#         lines = data.strip().split('\n')[1:]
        
#         snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        
#         for line in lines:
#             values = line.split(',')
            
#             # query for inserting records into snowflake table
#             query = f"""
#             INSERT INTO airflow_tasks (airline, avail_seat_km_per_week, incidents_85_99,fatal_accidents_85_99,fatalities_85_99,incidents_00_14,fatal_accidents_00_14,fatalities_00_14)
#             VALUES ('{values[0]}', '{values[1]}', '{values[2]}','{values[3]}','{values[4]}','{values[5]}','{values[6]}','{values[7]}')
#             """
            
#             snowflake_hook.run(query)
            
#         print("Data loaded into Snowflake successfully.")
#     else:
#         raise Exception(f"Failed to fetch data from URL. Status code: {response.status_code}")

# task2 = PythonOperator(
#     task_id='load_data_task',
#     python_callable=load_data_to_snowflake,
#     provide_context=True,
#     dag=dag,
# )

# task_1 >> task2



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
