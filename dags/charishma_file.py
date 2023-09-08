from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests
import csv
from pydantic import BaseModel, ValidationError

# Define Snowflake connection credentials
snowflake_credentials = {
    "account": "https://hzciyrm-kj91758.snowflakecomputing.com",
    "warehouse": "COMPUTE_WH",
    "database": "DEMO",
    "schema": "SC1",
    "username": "CJ",
    "password": "Cherry@2468"
}

# Create a function to establish the Snowflake connection using SnowflakeHook
def create_snowflake_connection():
    hook = SnowflakeHook(snowflake_conn_id="snow_sc")  
    conn = hook.get_conn()
    return conn

# Define the Pydantic model for CSV data
class CSVRecord(BaseModel):
    NAME: str
    EMAIL: str
    SSN: str

# Task to read file from provided URL and display data
def read_file_and_display_data():
    # Input CSV file URL
    csv_url = 'https://raw.githubusercontent.com/jcharishma/my.repo/master/sample_csv.csv'

    # Fetch CSV data from the URL
    response = requests.get(csv_url)
    if response.status_code == 200:
        csv_content = response.text
        print("CSV Data:")
        print(csv_content)
        return csv_content
    else:
        raise Exception(f"Failed to fetch CSV: Status Code {response.status_code}")

# Task to validate and load data using Pydantic
def validate_and_load_data():
    snowflake_conn = create_snowflake_connection()

    # Input CSV file URL
    csv_url = 'https://raw.githubusercontent.com/jcharishma/my.repo/master/sample_csv.csv'

    # Fetch CSV data from the URL
    response = requests.get(csv_url)
    if response.status_code == 200:
        csv_content = response.text
        csv_lines = csv_content.split('\n')
        csvreader = csv.DictReader(csv_lines)
        for row in csvreader:
            try:
                record = CSVRecord(**row)

                # Use SnowflakeOperator to insert data into Snowflake
                insert_task = SnowflakeOperator(
                    task_id='insert_into_sample_csv',
                    sql=f"""
                        INSERT INTO SAMPLE_CSV (NAME, EMAIL, SSN)
                        VALUES ('{record.NAME}', '{record.EMAIL}', '{record.SSN}')
                    """,
                    snowflake_conn_id="snowflake_conn_id",  # Connection ID defined in Airflow
                    dag=dag,
                )
                insert_task.execute(snowflake_conn)
            except ValidationError as e:
                for error in e.errors():
                    field_name = error.get('loc')[-1]
                    error_msg = error.get('msg')
                    print(f"Error in {field_name}: {error_msg}")
            except Exception as e:
                print(f"Error: {str(e)}")

    snowflake_conn.close()

# Airflow default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 7),
    'retries': 1,
    'catchup': True,
}

# Create the DAG
dag = DAG(
    'csv_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

# Task to read file from provided URL and display data
read_file_task = PythonOperator(
    task_id='read_file_and_display_data',
    python_callable=read_file_and_display_data,
    dag=dag,
)

# Task to validate and load data using Pydantic
validate_task = PythonOperator(
    task_id='validate_and_load_data',
    python_callable=validate_and_load_data,
    dag=dag,
)

# Set task dependencies
read_file_task >> validate_task




# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from datetime import datetime
# import requests
# from io import StringIO
# import pandas as pd
# import logging

# # Snowflake connection ID
# SNOWFLAKE_CONN_ID = 'snow_sc'

# default_args = {
#     'start_date': datetime(2023, 8, 25),
#     'retries': 1,
#     'catchup': True,
# }

# dag_args = {
#     'dag_id': 'charishma_csv_dag',
#     'schedule_interval': None,
#     'default_args': default_args,
#     'catchup': False,
# }
# dag = DAG(**dag_args)

# def read_file_from_url(**kwargs):
#     url = "https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv" 
#     response = requests.get(url)
#     data = response.text
#     kwargs['ti'].xcom_push(key='csv_data', value=data)  
#     print(f"Read data from URL. Content: {data}")
# def load_csv_data(**kwargs):
#     ti = kwargs['ti']
#     data = ti.xcom_pull(key='csv_data', task_ids='read_file_task')
#     df = pd.read_csv(StringIO(data))

#     # Convert 'SSN' column to string data type
#     df['SSN'] = df['SSN'].astype(str)

#     # Create a new column 'Invalid_SSN' and set it to None for all rows
#     df['Invalid_SSN'] = None

#     # Check if SSN values are exactly 4 digits and update 'Invalid_SSN' column for invalid rows
#     invalid_rows = df['SSN'].str.len() != 4
#     df.loc[invalid_rows, 'Invalid_SSN'] = df.loc[invalid_rows, 'SSN']

#     valid_rows = df[~invalid_rows]

#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
#     table_name = 'demo.sc1.sample_csv'

#     if not valid_rows.empty:
#         snowflake_hook.insert_rows(table_name, valid_rows.values.tolist(), valid_rows.columns.tolist())
#         print(f"Data load completed successfully for {len(valid_rows)} rows.")

#     if not invalid_rows.all():
#         print(f"Error: {invalid_rows.sum()} rows have invalid SSN and were not loaded to Snowflake.")
#         print("Invalid Rows:")
#         print(df.loc[invalid_rows])


# def load_csv_data(**kwargs):
#     ti = kwargs['ti']
#     data = ti.xcom_pull(key='csv_data', task_ids='read_file_task')  
#     df = pd.read_csv(StringIO(data))
    
#     # Convert 'SSN' column to string data type
#     df['SSN'] = df['SSN'].astype(str)
    
#     valid_rows = df[df['SSN'].str.len() == 4]
#     invalid_rows = df[df['SSN'].str.len() != 4]

#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
#     table_name = 'demo.sc1.sample_csv' 
    
#     if not valid_rows.empty:
#         snowflake_hook.insert_rows(table_name, valid_rows.values.tolist(), valid_rows.columns.tolist())
#         print(f"Data load completed successfully for {len(valid_rows)} rows.")
    
#     if not invalid_rows.empty:
#         print(f"Error: {len(invalid_rows)} rows have invalid SSN and were not loaded to Snowflake.")
#         print("Invalid Rows:")
#         print(invalid_rows)

# with dag:
#     read_file_task = PythonOperator(
#         task_id='read_file_task',
#         python_callable=read_file_from_url,
#         provide_context=True,
#     )

#     load_csv_data_task = PythonOperator(
#         task_id='load_csv_data',
#         python_callable=load_csv_data,
#         provide_context=True,
#     )

# read_file_task >> load_csv_data_task


# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from datetime import datetime
# import requests
# from io import StringIO
# import pandas as pd
# import logging

# # Snowflake connection ID
# SNOWFLAKE_CONN_ID = 'snow_sc'

# default_args = {
#     'start_date': datetime(2023, 8, 25),
#     'retries': 1,
#     'catchup': True,
# }

# dag_args = {
#     'dag_id': 'charishma_csv_dag',
#     'schedule_interval': None,
#     'default_args': default_args,
#     'catchup': False,
# }
# dag = DAG(**dag_args)

# def read_file_from_url(**kwargs):
#     url = "https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv" 
#     response = requests.get(url)
#     data = response.text
#     kwargs['ti'].xcom_push(key='csv_data', value=data)  # Push data as XCom variable
#     print(f"Read data from URL. Content: {data}")

# def load_csv_data(**kwargs):
#     ti = kwargs['ti']
#     data = ti.xcom_pull(key='csv_data', task_ids='read_file_task')  # Retrieve data from XCom
#     df = pd.read_csv(StringIO(data))
    
#     valid_rows = df[df['SSN'].astype(str).str.len() == 4]
#     invalid_rows = df[df['SSN'].astype(str).str.len() != 4]

#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
#     table_name = 'demo.sc1.sample_csv' 
    
#     if not valid_rows.empty:
#         snowflake_hook.insert_rows(table_name, valid_rows.values.tolist(), valid_rows.columns.tolist())
#         print(f"Data load completed successfully for {len(valid_rows)} rows.")
    
#     if not invalid_rows.empty:
#         print(f"Error: {len(invalid_rows)} rows have invalid SSN and were not loaded to Snowflake.")
#         print("Invalid Rows:")
#         print(invalid_rows)

# with dag:
#     read_file_task = PythonOperator(
#         task_id='read_file_task',
#         python_callable=read_file_from_url,
#         provide_context=True,
#     )

#     load_csv_data_task = PythonOperator(
#         task_id='load_csv_data',
#         python_callable=load_csv_data,
#         provide_context=True,
#     )

# read_file_task >> load_csv_data_task








# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import requests
# import csv

# # Snowflake connection ID
# # SNOWFLAKE_ID = 'snow_sc'

# default_args = {
#     'start_date': datetime(2023, 8, 31),
#     'catchup': False,
# }

# dag = DAG(
#     'charishma_csv_dag',
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
# )

# # Create a function to fetch data from the URL
# def fetch_data_from_url(**kwargs):
#     url = "https://github.com/jcharishma/my.repo/blob/master/sample_csv.csv"
#     response = requests.get(url)
#     response.raise_for_status()  

#     # Split the CSV data and skip the header row if present
#     data = []
#     for row in response.text.splitlines():
#         fields = row.split(',')
#         if len(fields) == 3:
#             name = fields[0]
#             email = fields[1]
#             ssn = fields[2]
            
#             # Check if SSN is exactly 4 digits
#             if ssn.isdigit() and len(ssn) == 4:
#                 data.append({'name': name, 'email': email, 'ssn': ssn})
#             else:
#                 print(f"Error: Invalid SSN detected in the CSV: {row}")

#     # Push the 'data' variable as an XCom value
#     kwargs['ti'].xcom_push(key='data', value=data)

# # Create the PythonOperator task to fetch data
# fetch_data_task = PythonOperator(
#     task_id='fetch_data',
#     python_callable=fetch_data_from_url,
#     provide_context=True,
#     dag=dag,
# )

# # Create a SnowflakeOperator task to load data into Snowflake
# snowflake_task = SnowflakeOperator(
#     task_id='load_data',
#     sql=f"COPY INTO sample_csv "
#     f"FROM 'https://raw.githubusercontent.com/jcharishma/my.repo/master/sample_csv.csv' "
#     f"FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);",
#     snowflake_conn_id='snow_sc',
#     autocommit=True,
#     depends_on_past=False,
#     dag=dag,
# )



# # Set up task dependencies
# fetch_data_task >> snowflake_task

# snowflake_task = SnowflakeOperator(
#     task_id='load_data',
#     sql=f"COPY INTO sample_csv "
#     f"FROM 'https://github.com/jcharishma/my.repo/blob/master/sample_csv.csv'"
#     f" FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);", 
#     snowflake_conn_id='snow_sc',
#     autocommit=True,
#     depends_on_past=False,
#     dag=dag,
# )



# from airflow import DAG
# from airflow.operators.python import PythonOperator, ShortCircuitOperator
# from datetime import datetime
# import pandas as pd
# import requests
# from io import StringIO
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# import os
# from airflow.models import Variable

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
#     if Variable.get('C_AIR_ENV') == 'True':
#         return True
#     else:
#         return False
# #     C_AIR_ENV = os.environ.get('C_AIR_ENV')
# #     print("C_AIR_ENV:", C_AIR_ENV) 
# #     print("Type of C_AIR_ENV:", type(C_AIR_ENV))  
# #     if C_AIR_ENV == 'True':
# #         print("Returning True")  
# #         return True
# #     else:
# #         print("Returning False") 
# #         return False



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
