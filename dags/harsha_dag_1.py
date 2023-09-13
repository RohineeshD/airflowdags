from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import requests
import io
import logging
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Airflow DAG configuration
dag = DAG(
    'load_csv_url_to_snowflake',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
)

def download_csv_and_load_to_snowflake():
    try:
        # URL to the CSV file
        csv_url = "https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv"

        # Attempt to download the CSV file
        response = requests.get(csv_url)
        response.raise_for_status()

        # Read the CSV data from the response content into a pandas DataFrame
        csv_data = pd.read_csv(io.StringIO(response.text))

        # Initialize the SnowflakeHook
        snowflake_hook = SnowflakeHook(snowflake_conn_id="air_conn")  

        # Snowflake table name
        snowflake_table = 'to_sql_table'


        # Define the batch size for insertion
        batch_size = 1000

        # Split the data into batches and insert into Snowflake
        for i in range(0, len(csv_data), batch_size):
            batch = csv_data[i:i+batch_size]
            engine = snowflake_hook.get_sqlalchemy_engine()
            batch.to_sql(name=snowflake_table, con=engine, if_exists='replace', index=False)

        logging.info('CSV data successfully loaded into Snowflake.')

    except Exception as e:
        # Handle the download or insertion error here
        logging.error(f'Error: {str(e)}')
        raise e

# Task to download the CSV file and load it into Snowflake
download_and_load_task = PythonOperator(
    task_id='download_and_load_csv',
    python_callable=download_csv_and_load_to_snowflake,
    dag=dag,
)

# Set task dependencies
download_and_load_task



# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.python import ShortCircuitOperator
# from airflow.utils.dates import days_ago
# from datetime import datetime
# import requests
# import pandas as pd
# import os
# import io

# # Define default_args for the DAG
# default_args = {
#     'owner': 'airflow',
#     'start_date': days_ago(1),
#     'schedule_interval': None,  
#     'catchup': False
# }

# dag = DAG(
#     'load_data_snowflake',
#     default_args=default_args,
#     description='Load CSV data into Snowflake',
#     catchup=False
# )

# # Define Snowflake connection ID from Airflow's Connection UI
# snowflake_conn_id = 'air_conn'

# def get_snowflake_hook(conn_id):
#     return SnowflakeHook(snowflake_conn_id=conn_id)


# # Define Snowflake target table
# snowflake_table = 'bulk_table'

# # Define the CSV URL
# csv_url = 'https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv'

# # Function to load CSV data into Snowflake
# def copy_csv_to_snowflake():
#     try:
#         snowflake_hook = get_snowflake_hook(snowflake_conn_id)

#         # Establish a Snowflake connection
#         conn = snowflake_hook.get_conn()
#         cursor = conn.cursor()


#         # Create a Snowflake internal stage for the CSV file
#         stage_name = 'csv_stage'
#         create_stage_sql = f'''
#         CREATE OR REPLACE STAGE {stage_name}
#         FILE_FORMAT = (
#             TYPE = 'CSV'
#             SKIP_HEADER = 1
#             FIELD_DELIMITER = ','
#             RECORD_DELIMITER = '\n'
#             FIELD_OPTIONALLY_ENCLOSED_BY = '"'
#         );
#         '''
#         cursor.execute(create_stage_sql)

#         # Download the CSV file to a local directory
#         response = requests.get(csv_url)
#         local_file_path = '/tmp/customers-100000.csv'
#         with open(local_file_path, 'wb') as file:
#             file.write(response.content)

#         # Upload the CSV file to the Snowflake internal stage
#         put_sql = f'''
#         PUT 'file://{local_file_path}' @{stage_name}
#         '''
#         cursor.execute(put_sql)
#         cursor.close()
#         conn.close()

#         # Snowflake COPY INTO command using the internal stage with error handling
#         snowflake_hook.run( 
#             f'''
#             COPY INTO {snowflake_table}
#             FROM @{stage_name}
#             FILE_FORMAT = (
#                 TYPE = 'CSV'
#                 SKIP_HEADER = 1
#                 FIELD_DELIMITER = ','
#                 RECORD_DELIMITER = '\n'
#                 FIELD_OPTIONALLY_ENCLOSED_BY = '"'
#             )
#             ON_ERROR = 'CONTINUE';
#             '''
#         )

#         # Drop the Snowflake internal stage after loading
#         snowflake_hook.run(f'DROP STAGE IF EXISTS {stage_name}')

#         print("Data loaded successfully")
#         return True
#     except Exception as e:
#         print("Data loading failed -", str(e))
#         return False



# # Task to truncate the Snowflake table before loading
# truncate_table_task = SnowflakeOperator(
#     task_id='truncate_snowflake_table_task',
#     sql=f'TRUNCATE TABLE {snowflake_table}',
#     snowflake_conn_id=snowflake_conn_id,
#     dag=dag
# )

# # Task to call the load_csv_to_snowflake function
# copy_csv_task = PythonOperator(
#     task_id='load_csv_to_snowflake_task',
#     python_callable=copy_csv_to_snowflake,
#     dag=dag
# )

# # task dependencies
# truncate_table_task >> copy_csv_task



# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.utils.dates import days_ago
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from io import StringIO
# import pandas as pd  
# import requests
# import logging

# default_args = {
#     'owner': 'airflow',
#     'start_date': days_ago(1),
#     'catchup': False,
#     'provide_context': True,
# }

# dag = DAG(
#     'harsha_dag1',
#     default_args=default_args,
#     schedule_interval=None,
# )

# # Define  Snowflake connection credentials
# SNOWFLAKE_CONN_ID = 'snowflake_creds'  
# SNOWFLAKE_SCHEMA = 'SCHEMA1'  
# STAGING_TABLE = 'stage_table'  
# MAIN_TABLE = 'main_table'  

# # Snowflake connection setup
# def get_snowflake_hook(conn_id):
#     return SnowflakeHook(snowflake_conn_id=conn_id)

# # Function to read data from the URL
# def read_data_from_url(**kwargs):
#     try:
#         url = "https://raw.githubusercontent.com/cs109/2014_data/master/countries.csv"
#         response = requests.get(url)
#         data = response.text
#         df = pd.read_csv(StringIO(data))
        
#         # Convert the DataFrame to a CSV 
#         csv_data = df.to_csv(index=False)

#         # Push the CSV data to XCom
#         kwargs['ti'].xcom_push(key='data_frame_csv', value=csv_data)  # Push the CSV data to XCom
#         return True
#     except Exception as e:
#         print(f"An error occurred while reading data: {str(e)}")
#         return False

# # Function to load data into Snowflake
# def load_data_into_snowflake(**kwargs):
#     try:
#         # Retrieve the CSV data from XCom
#         csv_data = kwargs['ti'].xcom_pull(key='data_frame_csv', task_ids='read_data_from_url')
        
#         # Convert the CSV data to a DataFrame
#         df = pd.read_csv(StringIO(csv_data))
        
#         # Upload DataFrame to Snowflake
#         snowflake_hook = get_snowflake_hook(SNOWFLAKE_CONN_ID)
#         connection = snowflake_hook.get_conn()
#         snowflake_hook.insert_rows(f'{SNOWFLAKE_SCHEMA}.{STAGING_TABLE}', df.values.tolist())
#         connection.close()
#         return True
#     except Exception as e:
#         print(f"An error occurred while loading data into Snowflake: {str(e)}")
#         return False

# # Function to check the data
# # def check_data(**kwargs):
# #     try:
        
# #         snowflake_hook = get_snowflake_hook(SNOWFLAKE_CONN_ID)
# #         connection = snowflake_hook.get_conn()
# #         cursor = connection.cursor()
# #         cursor.execute(f"SELECT * FROM {SNOWFLAKE_SCHEMA}.{STAGING_TABLE}")
# #         result = cursor.fetchall()
# #         cursor.close()
# #         connection.close()
        
# #         return True
# #     except Exception as e:
# #         print(f"An error occurred while checking data: {str(e)}")
# #         return False

# # Task 1: Read data from the URL
# read_data_task = PythonOperator(
#     task_id='read_data_from_url',
#     python_callable=read_data_from_url,
#     provide_context=True,
#     dag=dag,
# )

# # Task 2: Load data into Snowflake
# load_data_task = PythonOperator(
#     task_id='load_data_into_snowflake',
#     python_callable=load_data_into_snowflake,
#     provide_context=True,
#     dag=dag,
# )

# # # Task 3: Check the data
# # check_data_task = PythonOperator(
# #     task_id='check_data',
# #     python_callable=check_data,
# #     provide_context=True,
# #     dag=dag,
# # )

# # trigger_dag_2 = TriggerDagRunOperator(
# #     task_id='trigger_dag_2',
# #     trigger_dag_id="harsha_dag2",
# #     dag=dag,
# # )


# read_data_task >> load_data_task 
# # >> check_data_task >> trigger_dag_2 '  




# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.utils.dates import days_ago
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from io import StringIO
# import pandas as pd  
# import requests
# import logging

# default_args = {
#     'owner': 'airflow',
#     'start_date': days_ago(1),
#     'catchup': False,
#     'provide_context': True,
# }

# dag = DAG(
#     'harsha_dag1',
#     default_args=default_args,
#     schedule_interval=None,
# )

# # Define  Snowflake connection credentials
# SNOWFLAKE_CONN_ID = 'snowflake_conn'  
# SNOWFLAKE_SCHEMA = 'exusia_schema'  
# STAGING_TABLE = 'stage_table'  
# MAIN_TABLE = 'main_table'  

# # Snowflake connection setup
# def get_snowflake_hook(conn_id):
#     return SnowflakeHook(snowflake_conn_id=conn_id)

# # Function to read data from the URL
# def read_data_from_url(**kwargs):
#     try:
#         url = "https://raw.githubusercontent.com/cs109/2014_data/master/countries.csv"
#         response = requests.get(url)
#         data = response.text
#         df = pd.read_csv(StringIO(data))
        
#         # Convert the DataFrame to a CSV 
#         csv_data = df.to_csv(index=False)

#         # Push the CSV data to XCom
#         kwargs['ti'].xcom_push(key='data_frame_csv', value=csv_data)  # Push the CSV data to XCom
#         return True
#     except Exception as e:
#         print(f"An error occurred while reading data: {str(e)}")
#         return False

# # Function to load data into Snowflake
# def load_data_into_snowflake(**kwargs):
#     try:
#         # Retrieve the CSV data from XCom
#         csv_data = kwargs['ti'].xcom_pull(key='data_frame_csv', task_ids='read_data_from_url')
        
#         # Convert the CSV data to a DataFrame
#         df = pd.read_csv(StringIO(data))
        
#         # Upload DataFrame to Snowflake
#         snowflake_hook = get_snowflake_hook(SNOWFLAKE_CONN_ID)
#         connection = snowflake_hook.get_conn()
#         snowflake_hook.insert_rows(f'{SNOWFLAKE_SCHEMA}.{STAGING_TABLE}', df.values.tolist())
#         connection.close()
#         return True
#     except Exception as e:
#         print(f"An error occurred while loading data into Snowflake: {str(e)}")
#         return False

# # Function to check the data
# def check_data(**kwargs):
#     try:
        
#         snowflake_hook = get_snowflake_hook(SNOWFLAKE_CONN_ID)
#         connection = snowflake_hook.get_conn()
#         cursor = connection.cursor()
#         cursor.execute(f"SELECT * FROM {SNOWFLAKE_SCHEMA}.{STAGING_TABLE}")
#         result = cursor.fetchall()
#         cursor.close()
#         connection.close()
        
#         return True
#     except Exception as e:
#         print(f"An error occurred while checking data: {str(e)}")
#         return False

# # Task 1: Read data from the URL
# read_data_task = PythonOperator(
#     task_id='read_data_from_url',
#     python_callable=read_data_from_url,
#     provide_context=True,
#     dag=dag,
# )

# # Task 2: Load data into Snowflake
# load_data_task = PythonOperator(
#     task_id='load_data_into_snowflake',
#     python_callable=load_data_into_snowflake,
#     provide_context=True,
#     dag=dag,
# )

# # Task 3: Check the data
# check_data_task = PythonOperator(
#     task_id='check_data',
#     python_callable=check_data,
#     provide_context=True,
#     dag=dag,
# )

# trigger_dag_2 = TriggerDagRunOperator(
#     task_id='trigger_dag_2',
#     trigger_dag_id="harsha_dag2",
#     dag=dag,
# )


# read_data_task >> load_data_task >> check_data_task >> trigger_dag_2 


# from datetime import datetime
# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# # # Define the DAG
# default_args = {
#      'owner': 'airflow',
#      'start_date': datetime(2023, 9, 1),
#      'retries': 1,
# }

# dag = DAG(
#      'harsha_dag2',
#      default_args=default_args,
#      schedule_interval=None,  
#      catchup=False,
    
# )

# # # Task 1: Load Data from Staging Table to Main
# def load_data():
#      try:
#          snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
#          conn = snowflake_hook.get_conn()
#          cursor = conn.cursor()

        
#          sql_query = """
#          INSERT INTO main_harsha (Country, Region)
#          SELECT Country, Region
#          FROM stage_harsha limit 10;
#          """

#          cursor.execute(sql_query)
#          cursor.close()
#          conn.close()
#          print("Data loaded successfully")
#      except Exception as e:
#          print("Data loading failed -", str(e))

# # # Task 2: Check if Load is Successful
# def check_load_status():
#      try:
#          snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
#          conn = snowflake_hook.get_conn()
#          cursor = conn.cursor()

        
#          sql_query = "SELECT COUNT(*) FROM main_harsha;"
#          cursor.execute(sql_query)
#          row = cursor.fetchone()

#          if row[0] > 0:
#              print("Load was successful")
#              return True
#          else:
#              print("Load failed - Main table is empty")
#              return False
#      except Exception as e:
#          print("Load failed -", str(e))
#          return False

# # # Task 3: Print Success or Failure Status
# def print_status(load_success):
#      if load_success:
#          print("Success")
#      else:
#          print("Failure")


# # # Task 1: Load Data
# load_data_task = PythonOperator(
#      task_id='load_data',
#      python_callable=load_data,
#      dag=dag,
# )

# # # Task 2: Check Load Status
# check_load_status_task = PythonOperator(
#      task_id='check_load_status',
#      python_callable=check_load_status,
#      provide_context=True,
#      dag=dag,
# )

# # # Task 3: Print Status
# print_status_task = PythonOperator(
#      task_id='print_status',
#      python_callable=print_status,
#      op_args=[check_load_status_task.output],
#      provide_context=True,
#      dag=dag,
# )


# load_data_task >> check_load_status_task >> print_status_task

