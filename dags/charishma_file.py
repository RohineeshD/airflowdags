# from airflow import DAG
# from airflow.operators.python import PythonOperator, ShortCircuitOperator
# from datetime import datetime
# import pandas as pd
# import requests
# from io import StringIO
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# import os
from airflow.operators.bash_operator import BashOperator

log_env_task = BashOperator(
    task_id='log_env_variables',
    bash_command='env',
    provide_context=True,
)

check_env_task >> log_env_task


# # global Snowflake connection ID
# SNOWFLAKE_CONN_ID = 'snow_sc'

# default_args = {
#     'start_date': datetime(2023, 8, 25),
#     'retries': 1,
# }

# # def check_env_variable(**kwargs):
# #     C_AIR_ENV = os.environ.get('C_AIR_ENV')
# #     if C_AIR_ENV == 'True':
# #         return True  # ShortCircuitOperator should return True to proceed with downstream tasks
# #     else:
# #         return False
# def check_env_variable(**kwargs):
#     C_AIR_ENV = os.environ.get('C_AIR_ENV')
#     print("C_AIR_ENV:", C_AIR_ENV)  # Add this line
#     if C_AIR_ENV == 'True':
#         print(" Returning True")  
#         return True
#     else:
#         print("Returning False") 
#         return False


# def fetch_csv_and_upload(**kwargs):
#     url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
#     response = requests.get(url)
#     data = response.text
#     df = pd.read_csv(StringIO(data))
#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
#     table_name = 'air_local'
    
#     snowflake_hook.insert_rows(table_name, df.values.tolist(), df.columns.tolist())

# def filter_records(**kwargs):
#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
#     sql_task3 = """
#     SELECT *
#     FROM air_local
#     WHERE avail_seat_km_per_week > 698012498
#     """
    
#     result = snowflake_hook.get_records(sql_task3)
#     num_records = 10 if result else 5
    
#     return num_records

# def print_records(num_records, **kwargs):
#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
#     sql_task4 = f"""
#     SELECT *
#     FROM air_local
#     WHERE avail_seat_km_per_week > 698012498
#     LIMIT {num_records}
#     """
    
#     records = snowflake_hook.get_records(sql_task4)
#     print("Printing records:")
#     print(records)
    
# def final_task(**kwargs):
#     print("Processes completed successfully.")

# # Define the DAG
# with DAG('charishma_dags', schedule_interval=None, default_args=default_args) as dag:
#     check_env_task = ShortCircuitOperator(
#         task_id='check_env_variable',
#         python_callable=check_env_variable,
#         provide_context=True,
#     )

#     upload_data_task = PythonOperator(
#         task_id='fetch_csv_and_upload',
#         python_callable=fetch_csv_and_upload,
#         provide_context=True,
#     )
    
#     num_records_task = PythonOperator(
#         task_id='filter_records',
#         python_callable=filter_records,
#         provide_context=True,
#     )
    
#     print_records_task = PythonOperator(
#         task_id='print_records',
#         python_callable=print_records,
#         op_args=[num_records_task.output],  
#         provide_context=True,
#     )
    
#     final_print_task = PythonOperator(
#         task_id='final_print_task',
#         python_callable=final_task,
#         provide_context=True,
#     )

#     # Set task dependencies
#     check_env_task >> upload_data_task >> num_records_task >> print_records_task >> final_print_task















# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime
# import pandas as pd
# import requests
# from io import StringIO
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# import os

# # global Snowflake connection ID
# SNOWFLAKE_CONN_ID = 'snow_sc'

# default_args = {
#     'start_date': datetime(2023, 8, 25),
#     'retries': 1,
# }

# def check_env_variable(**kwargs):
#     c_air_env = os.environ.get('C_AIR_ENV')
#     if c_air_env == 'true':
#         return 'fetch_csv_and_upload'

# def fetch_csv_and_upload(**kwargs):
#     url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
#     response = requests.get(url)
#     data = response.text
#     df = pd.read_csv(StringIO(data))
    
#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
#     table_name = 'air_table'
    
#     snowflake_hook.insert_rows(table_name, df.values.tolist(), df.columns.tolist())

# def filter_records(**kwargs):
#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
#     sql_task3 = """
#     SELECT *
#     FROM air_table
#     WHERE avail_seat_km_per_week > 698012498
#     """
    
#     result = snowflake_hook.get_records(sql_task3)
#     num_records = 10 if result else 5
    
#     return num_records

# def print_records(num_records, **kwargs):
#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
#     sql_task4 = f"""
#     SELECT *
#     FROM air_table
#     WHERE avail_seat_km_per_week > 698012498
#     LIMIT {num_records}
#     """
    
#     records = snowflake_hook.get_records(sql_task4)
#     print("Printing records:")
#     print(records)
    
#     # Task 5: Print process completed
#     print("Process completed")

# with DAG('charishma_dags', schedule_interval=None, default_args=default_args) as dag:
#     check_env_task = PythonOperator(
#         task_id='check_env_variable',
#         python_callable=check_env_variable,
#         provide_context=True,
#     )

#     upload_data_task = PythonOperator(
#         task_id='fetch_csv_and_upload',
#         python_callable=fetch_csv_and_upload,
#         provide_context=True,
#     )
    
#     num_records_task = PythonOperator(
#         task_id='filter_records',
#         python_callable=filter_records,
#         provide_context=True,
#     )
    
#     print_records_task = PythonOperator(
#         task_id='print_records',
#         python_callable=print_records,
#         op_args=[num_records_task.output],  # Pass the output of num_records_task
#         provide_context=True,
#     )

#     check_env_task >> [upload_data_task, num_records_task, print_records_task]









# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime
# import pandas as pd
# import requests
# from io import StringIO
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# default_args = {
#     'start_date': datetime(2023, 8, 25),
#     'retries': 1,
# }

# def fetch_csv_and_upload(**kwargs):
#     try:
#         url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
#         response = requests.get(url)
        
#         if response.status_code == 200:
#             data = response.text
#             df = pd.read_csv(StringIO(data))
            
#             # Upload DataFrame to Snowflake
#             snowflake_hook = SnowflakeHook(snowflake_conn_id='snow_sc')
#             table_name = 'airflow_tasks'
#             snowflake_hook.insert_rows(table_name, df.values.tolist(), df.columns.tolist())
#             print("Data uploaded successfully.")
#         else:
#             print("Failed to fetch data from the URL. Status code:", response.status_code)
#     except Exception as e:
#         print("An error occurred:", str(e))

# with DAG('charishma_dags', schedule_interval=None, default_args=default_args) as dag:
#     upload_data_task = PythonOperator(
#         task_id='fetch_csv_and_upload',
#         python_callable=fetch_csv_and_upload,
#         provide_context=True,
#     )

#     upload_data_task






# from airflow import DAG
# from airflow.operators.python import BranchPythonOperator
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from datetime import datetime
# import os

# default_args = {
#     'start_date': datetime(2023, 8, 25),
#     'retries': 1,
# }

# def check_env_variable(**kwargs):
#     c_air_env = os.environ.get('C_AIR_ENV')
#     print(f"Value of C_AIR_ENV: {c_air_env}")
#     if c_air_env == 'true':
#         return 'load_data_task'
#     return None
# # def check_env_variable():
# #     if os.environ.get('C_AIR_ENV') == 'true':
# #         return 'load_data_task'
# #         return None
# with DAG('charishma_dags', schedule_interval=None, default_args=default_args) as dag:
#     check_env_task = BranchPythonOperator(
#         task_id='check_env_variable',
#         python_callable=check_env_variable,
#         provide_context=True,
#     )

#     load_data_task = SnowflakeOperator(
#         task_id='load_data_task',
#         sql=f"COPY INTO airflow_tasks "
#     f"FROM 'https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv'"
#     f" FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);", 
#         snowflake_conn_id='snow_sc',
#     )

#     check_env_task >> load_data_task


# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime



# # Step 2: Initiating the default_args
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2022, 11, 12),
# }

# # Define a Python function to be executed by the PythonOperator
# def print_hello():
#     print("Welcome to Charishma's dag!")

# # Step 3: Creating DAG Object
# dag = DAG(
#     dag_id='charishma_dag',
#     default_args=default_args,
#     schedule_interval='@once',  
#     catchup=False,
# )

# # Step 4: Creating task
# # Create a PythonOperator that will run the print_hello function
# task = PythonOperator(
#     task_id='print_welcome',
#     python_callable=print_hello,
#     dag=dag,
# )


# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from datetime import datetime

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 1, 1),
#     'retries': 1,
# }

# dag = DAG(
#     'charishma_dags',  
#     default_args=default_args,
#     schedule_interval='@once',
#     catchup=False,
# )

# sql_query = """
# SELECT max(id) AS max_id
# FROM table1
# """

# snowflake_task = SnowflakeOperator(
#     task_id='execute_snowflake_query',
#     sql=sql_query,
#     snowflake_conn_id='snow_conn',
#     autocommit=True,
#     dag=dag,
# )




# # Step 1: Importing Modules
# # To initiate the DAG Object
# from airflow import DAG
# # Importing datetime and timedelta modules for scheduling the DAGs
# from datetime import timedelta, datetime
# # Importing operators 
# from airflow.operators.dummy_operator import DummyOperator

# # Step 2: Initiating the default_args
# default_args = {
#         'owner' : 'airflow',
#         'start_date' : datetime(2022, 11, 12),

# }

# # Step 3: Creating DAG Object
# dag = DAG(dag_id='charishma_dag',
#         default_args=default_args,
#         schedule_interval='@once', 
#         catchup=False
#     )

# # Step 4: Creating task
# # Creating first task
# start = DummyOperator(task_id = 'start', dag = dag)
# # Creating second task 
# end = DummyOperator(task_id = 'end', dag = dag)

#  # Step 5: Setting up dependencies 
# start >> end 
