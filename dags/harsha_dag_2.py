# # monitor_file_arrival.py
# from watchdog.observers import Observer
# from watchdog.events import FileSystemEventHandler
# import subprocess

# class FileArrivalHandler(FileSystemEventHandler):
#     def on_created(self, event):
#         if not event.is_directory:
#             # Trigger the Airflow DAG when a new file arrives
#             subprocess.call(["airflow", "trigger_dag", "load_local_file_to_snowflake"])

# def start_file_monitoring():
#     path = "C:/Users/User/Desktop"  
#     event_handler = FileArrivalHandler()
#     observer = Observer()
#     observer.schedule(event_handler, path, recursive=False)
#     observer.start()
#     observer.join()

# if __name__ == "__main__":
#     start_file_monitoring()

# load_local_file_to_snowflake_dag.py
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import os

# Define your DAG
dag = DAG(
    'load_local_file_to_snowflake',
    schedule_interval=None,  
    start_date=datetime(2023, 9, 18),  
    catchup=False,  
)

# Define a PythonOperator to check for file arrival
def check_file_arrival():
    directory = 'C:/Users/User/Desktop/load'  
    file_name = 'Downloaded_CSV_TABLE.csv'
    full_file_path = os.path.join(directory, file_name)
    
    if os.path.exists(full_file_path):
        return "load_local_file_task"  # Trigger the Snowflake task if the file exists
    else:
        return "no_files"

check_for_file_task = PythonOperator(
    task_id='check_for_file_arrival',
    python_callable=check_file_arrival,
    provide_context=True,
    dag=dag,
)

# Define a DummyOperator task for when no files are present
no_files_task = DummyOperator(
    task_id='no_files',
    dag=dag,
)

# Define the SnowflakeOperator task to create the stage and load the file
create_snowflake_stage_task = SnowflakeOperator(
    task_id='create_snowflake_stage',
    sql=[
        "CREATE OR REPLACE stage snowflake_stage",  
        "COPY INTO automate_table FROM 'C:/Users/User/Desktop/load/Downloaded_CSV_TABLE.csv' FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)"  
    ],
    snowflake_conn_id='air_conn',
    autocommit=True,
    trigger_rule='one_success',  
    dag=dag,
)

# Set up task dependencies
check_for_file_task >> create_snowflake_stage_task
no_files_task >> create_snowflake_stage_task  # In case there are no files, still create the stage

if __name__ == "__main__":
    dag.cli()







# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# import pandas as pd
# import requests
# import io
# import logging
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# # Airflow DAG configuration
# dag = DAG(
#     'load_csv_from_url_to_snowflake',
#     start_date=datetime(2023, 1, 1),
#     schedule_interval=None,
#     catchup=False,
# )

# def download_csv_and_load_to_snowflake():
#     try:
#         # URL to the CSV file
#         csv_url = "https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv"

#         # Attempt to download the CSV file
#         response = requests.get(csv_url)
#         response.raise_for_status()

#         # Read the CSV data from the response content into a pandas DataFrame
#         csv_data = pd.read_csv(io.StringIO(response.text))

#         # Initialize the SnowflakeHook
#         snowflake_hook = SnowflakeHook(snowflake_conn_id="air_conn")  

#         # Snowflake table name
#         snowflake_table = 'is_sql_table'


#         # Define the batch size for insertion
#         batch_size = 1000

#         # Split the data into batches and insert into Snowflake
#         for i in range(0, len(csv_data), batch_size):
#             batch = csv_data[i:i+batch_size]
#             engine = snowflake_hook.get_sqlalchemy_engine()
#             batch.to_sql(name=snowflake_table, con=engine, if_exists='append', index=False)

#         logging.info('CSV data successfully loaded into Snowflake.')

#     except Exception as e:
#         # Handle the download or insertion error here
#         logging.error(f'Error: {str(e)}')
#         raise e

# # Task to download the CSV file and load it into Snowflake
# download_and_load_task = PythonOperator(
#     task_id='download_and_load_csv',
#     python_callable=download_csv_and_load_to_snowflake,
#     dag=dag,
# )

# # Set task dependencies
# download_and_load_task


# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# import pandas as pd
# import numpy as np
# import requests
# import io
# import logging
# from airflow.hooks.base_hook import BaseHook  
# from sqlalchemy import create_engine
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# # Airflow DAG configuration
# dag = DAG(
#     'load_csv_from_url_to_snowflake',
#     start_date=datetime(2023, 1, 1),
#     schedule_interval=None,
#     catchup=False,
# )

# def download_csv_and_load_to_snowflake():
#     try:
#         # URL to the CSV file
#         csv_url = "https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv"

#         # Attempt to download the CSV file
#         response = requests.get(csv_url)
#         response.raise_for_status()

#         # Read the CSV data from the response content into a pandas DataFrame
#         csv_data = pd.read_csv(io.StringIO(response.text))

        
#         snowflake_hook = SnowflakeHook(snowflake_conn_id="air_conn")

#         # Create a Snowflake connection using SQLAlchemy and the connection URL
#         # snowflake_engine = create_engine(snowflake_conn.get_uri())

#         # Snowflake table
#         snowflake_table = 'is_sql_table'
        
#         engine = snowflake_hook.get_sqlalchemy_engine()

#         # Insert data into the Snowflake table using SQLAlchemy
#         csv_data.to_sql(name=snowflake_table, con=engine, if_exists='replace', index=False)

#         logging.info('CSV data successfully loaded into Snowflake.')

#     except Exception as e:
#         # Handle the download or insertion error here
#         logging.error(f'Error: {str(e)}')
#         raise e

# # Task to download the CSV file and load it into Snowflake
# download_and_load_task = PythonOperator(
#     task_id='download_and_load_csv',
#     python_callable=download_csv_and_load_to_snowflake,
#     dag=dag,
# )

# # Set task dependencies 
# download_and_load_task


# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# import pandas as pd
# import numpy as np
# import requests
# import io
# import logging
# from sqlalchemy import create_engine
# from snowflake.sqlalchemy import URL  # Import Snowflake URL

# # Airflow DAG configuration
# dag = DAG(
#     'load_csv_from_url_to_snowflake',
#     start_date=datetime(2023, 1, 1),
#     schedule_interval=None,
#     catchup=False,
# )

# def download_csv_and_load_to_snowflake():
#     try:
#         # URL to the CSV file
#         csv_url = "https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv"

#         # Attempt to download the CSV file
#         response = requests.get(csv_url)
#         response.raise_for_status()

#         # Read the CSV data from the response content into a pandas DataFrame
#         csv_data = pd.read_csv(io.StringIO(response.text))

#         # Create a Snowflake connection using SQLAlchemy and the connection URL
#         snowflake_engine = create_engine(URL(
#             account='smdjtrh-gc37630',
#             user='harsha',
#             password='Rama@342',
#             database='exusia_db',
#             schema='exusia_schema',
#             warehouse='compute_wh',
#             role='accountadmin',
#             numpy=True,
#         ))

#         # Snowflake table
#         snowflake_table = 'is_sql_table'

#         # Insert data into the Snowflake table using SQLAlchemy
#         csv_data.to_sql(name=snowflake_table, con=snowflake_engine, if_exists='replace', index=False)

#         logging.info('CSV data successfully loaded into Snowflake.')

#     except Exception as e:
#         # Handle the download or insertion error here
#         logging.error(f'Error: {str(e)}')
#         raise e

# # Task to download the CSV file and load it into Snowflake
# download_and_load_task = PythonOperator(
#     task_id='download_and_load_csv',
#     python_callable=download_csv_and_load_to_snowflake,
#     dag=dag,
# )

# # Set task dependencies (no need for an HTTP sensor)
# download_and_load_task



# import requests
# from snowflake.connector import SnowflakeConnection, ProgrammingError
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
#     'load_snowflake',
#     default_args=default_args,
#     description='Load CSV data into Snowflake',
#     catchup=False
# )

# table_name ='traditional_insert'

# snowflake_conn_id ='air_conn'

# csv_url = "https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv"

# def insert_data_to_snowflake(table_name, snowflake_conn_id, csv_url):
#     try:
#         response = requests.get(csv_url)

#         if response.status_code == 200:
#             data = response.text
#             lines = data.strip().split('\n')[1:]
#             snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

#             # Truncate the table before loading new data
#             truncate_query = f"TRUNCATE TABLE {table_name}"
#             snowflake_hook.run(truncate_query)

  
#             for line in lines:
#                 values = line.split(',')
#                 if len(values) >= 13:
#                      # Remove double quotes from values
#                     values = [v.strip('"').strip() for v in values]
#                     # params = tuple(values)  # Convert values to a tuple
#                     query = f"""
#                         INSERT INTO {table_name} (Index, CustomerId, FirstName, LastName, Company, City, Country, Phone1, Phone2, Email, SubscriptionDate, Website)
                        
#                         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        
#                     """
             
#                     params = tuple(values[1:13])
                    
#                 else:
#                     print("Not enough elements in the 'values' list.")
#                     continue
#                     print("Skipping row with insufficient columns.")

       

#             print("Data loaded into Snowflake successfully.")
#         else:
#             raise Exception(f"Failed to fetch data from URL. Status code: {response.status_code}")
#     except Exception as e:
#         print(f"An error occurred: {str(e)}")

# # Usage example
# insert_data_to_snowflake("table_name", "snowflake_conn_id", "csv_url")

# insert_data_task = PythonOperator(
#     task_id='load_data_task',
#     python_callable=insert_data_to_snowflake,
#     op_args=[table_name, snowflake_conn_id, csv_url],
#     provide_context=True,
#     dag=dag,
# )

# insert_data_task



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
#     'load_snowflake',
#     default_args=default_args,
#     description='Load CSV data into Snowflake',
#     catchup=False
# )

# # Define Snowflake connection ID from Airflow's Connection UI
# snowflake_conn_id = 'air_conn'

# def get_snowflake_hook(conn_id):
#     return SnowflakeHook(snowflake_conn_id=conn_id)

# def insert_data_to_snowflake(**kwargs):
#     url = "https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv"
#     response = requests.get(url)
    
#     if response.status_code == 200:
#         data = response.text
#         lines = data.strip().split('\n')[1:]
#         snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

#         # Truncate the table before loading new data
#         truncate_query = "TRUNCATE TABLE airflow_tasks"
#         snowflake_hook.run(truncate_query)
        
#         for line in lines:
#             values = line.split(',')
#             query = f"""
#                 INSERT INTO traditional_insert (Index, CustomerId, FirstName, LastName, Company, City, Country, Phone1,Phone2,Email,SubscriptionDate,Website)
#                 VALUES ( '{values[1]}', '{values[2]}', '{values[3]}', '{values[4]}', '{values[5]}', '{values[6]}', '{values[8]}','{values[9]}','{values[10]}','{values[11]}','{values[12]}')
#             """
#             snowflake_hook.run(query)
            
#         print("Data loaded into Snowflake successfully.")
#     else:
#         raise Exception(f"Failed to fetch data from URL. Status code: {response.status_code}")

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


# insert_data_task = PythonOperator(
#     task_id='load_data_task',
#     python_callable=insert_data_to_snowflake,
#     provide_context=True,
#     dag=dag,
# )


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
# insert_data_task >> truncate_table_task >> copy_csv_task









# =========================================================working=================================================
# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# from datetime import datetime
# import requests

# # Define default_args for the DAG
# default_args = {
#     'owner': 'airflow',
#     'start_date': days_ago(1),
#     'schedule_interval': None,  
#     'catchup': False
# }

# dag = DAG(
#     'load_snowflake',
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
# def load_csv_to_snowflake():
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
                # TYPE = 'CSV'
                # SKIP_HEADER = 1

#             )
#             ON_ERROR = 'CONTINUE';
#             '''
#         )

#         # Drop the Snowflake internal stage after loading
#         snowflake_hook.run( f'DROP STAGE IF EXISTS {stage_name}')

#         print("Data loaded successfully")
#         return True
#     except Exception as e:
#         print("Data loading failed -", str(e))
#         return False

# # Task to call the load_csv_to_snowflake function
# load_csv_task = PythonOperator(
#     task_id='load_csv_to_snowflake_task',
#     python_callable=load_csv_to_snowflake,
#     dag=dag
# )


# if __name__ == "__main__":
#     dag.cli()











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
#      'harsh_dag',
#      default_args=default_args,
#      schedule_interval=None,  
#      catchup=False,
    
# )

# def load_data():
#     try:
#         snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
#         conn = snowflake_hook.get_conn()
#         cursor = conn.cursor()

#         # Truncate the main_table before loading data
#         truncate_query = """
#         TRUNCATE TABLE main_table;
#         """
#         cursor.execute(truncate_query)

#         # Insert data from stage_table into main_table
#         sql_query = """
#         INSERT INTO main_table (Country, Region)
#         SELECT Country, Region
#         FROM stage_table;
#         """
#         cursor.execute(sql_query)

#         cursor.close()
#         conn.close()
#         print("Data loaded successfully")
#         return True
#     except Exception as e:
#         print("Data loading failed -", str(e))
#         return False

# # # Task 2: Check if Load is Successful
# def check_load_status():
#      try:
#          snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
#          conn = snowflake_hook.get_conn()
#          cursor = conn.cursor()

        
#          sql_query = "SELECT * FROM main_table;"
#          cursor.execute(sql_query)
#          row = cursor.fetchall()

        
#          print("Load was successful")
#          return True
             
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

# def load_data():
#     try:
#         snowflake_hook = SnowflakeHook(snowflake_conn_id='air_conn')
#         conn = snowflake_hook.get_conn()
#         cursor = conn.cursor()

#         # Truncate the main_table before loading data
#         truncate_query = """
#         TRUNCATE TABLE main_table;
#         """
#         cursor.execute(truncate_query)

#         # Insert data from stage_table into main_table
#         sql_query = """
#         INSERT INTO main_table (Country, Region)
#         SELECT Country, Region
#         FROM stage_table;
#         """
#         cursor.execute(sql_query)

#         cursor.close()
#         conn.close()
#         print("Data loaded successfully")
#         return True
#     except Exception as e:
#         print("Data loading failed -", str(e))
#         return False


# # # Task 1: Load Data from Staging Table to Main
# # def load_data():
# #      try:
# #          snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
# #          conn = snowflake_hook.get_conn()
# #          cursor = conn.cursor()

        
# #          sql_query = """
# #          INSERT INTO main_table (Country, Region)
# #          SELECT Country, Region
# #          FROM stage_table;
# #          """

# #          cursor.execute(sql_query)
# #          cursor.close()
# #          conn.close()
# #          print("Data loaded successfully")
# #          return True
# #      except Exception as e:
# #          print("Data loading failed -", str(e))
# #          return False

# # # Task 2: Check if Load is Successful
# def check_load_status():
#      try:
#          snowflake_hook = SnowflakeHook(snowflake_conn_id='air_conn')
#          conn = snowflake_hook.get_conn()
#          cursor = conn.cursor()

        
#          sql_query = "SELECT * FROM main_table;"
#          cursor.execute(sql_query)
#          row = cursor.fetchall()

        
#          print("Load was successful")
#          return True
             
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
