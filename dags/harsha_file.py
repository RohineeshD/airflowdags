from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime
import requests
import pandas as pd
import io

default_args = {
    'start_date': datetime(2023, 8, 31),
    'catchup': False,
}

dag_1 = DAG(
    'dag_1_harsha',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)


def load_data_to_snowflake(**kwargs):
    url =  "https://raw.githubusercontent.com/cs109/2014_data/master/countries.csv"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.text
        lines = data.strip().split('\n')[1:]
        snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        
        for line in lines:
            values = line.split(',')
            # country = values[0].strip()
            # region = values[1].strip()
            query = f"""
                INSERT INTO stage_harsha (Contry, Region)
                # VALUES ('{country[0]}', '{region[1]}')
                VALUES ('{values[0]}', '{values[1]}')
            """
            snowflake_hook.run(query)
            
        print("Data loaded into Snowflake successfully.")
    else:
        raise Exception(f"Failed to fetch data from URL. Status code: {response.status_code}")

task_1 = PythonOperator(
    task_id='load_data_task',
    python_callable=load_data_to_snowflake,
    provide_context=True,
    dag=dag_1,
)

task_1


# # Trigger the loading task only if the reading task succeeds
# trigger_load_data_task = TriggerDagRunOperator(
#     task_id='trigger_load_data',
#     trigger_dag_id="dag_1_hars",
#     dag=dag_1,
# )

# read_data_task >> trigger_load_data_task


# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.operators.python_operator import PythonOperator
# from airflow import DAG
# from datetime import datetime
# import requests
# import pandas as pd
# import io

# default_args = {
#     'start_date': datetime(2023, 8, 31),
#     'catchup': False,
# }

# dag_1 = DAG(
#     'dag_1_hars',
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
# )

# # Task 1: Read data from URL using Pandas
# def read_data():
#     url = "https://raw.githubusercontent.com/cs109/2014_data/master/countries.csv"
#     response = requests.get(url)
#     print(f"Status code: {response.status_code}")

#     if response.status_code == 200:
#         data = response.text
#         print("Fetched data from URL:")
#         print(data)

#         df = pd.read_csv(io.StringIO(data))
#         return df
#     else:
#         raise Exception(f"Failed to fetch data from URL. Status code: {response.status_code}")

# read_data_task = PythonOperator(
#     task_id='read_data',
#     python_callable=read_data,
#     dag=dag_1,
# )

# # Task 2: Load data into Snowflake using SnowflakeHook
# def load_data(**kwargs):
#     ti = kwargs['ti']
#     data = ti.xcom_pull(task_ids='read_data')
#     snowflake_hook = SnowflakeHook(snowflake_conn_id='snow_sc') 
#     connection = snowflake_hook.get_conn()
#     cursor = connection.cursor()

#     try:
#         database_name = "demo"
#         schema_name = "sc1"
#         table_name = "stage_harsha"

#         df = pd.read_csv(io.StringIO(data))

#         # Convert DataFrame to a list of tuples
#         records = df.values.tolist()

#         # Truncate the table before inserting new data (optional)
#         # cursor.execute(f"TRUNCATE TABLE {database_name}.{schema_name}.{table_name}")

#         # Use COPY INTO to load data into Snowflake efficiently
#         cursor.executemany(f"INSERT INTO {database_name}.{schema_name}.{table_name} (column1, column2) VALUES (?, ?)", records)

#         # Commit the changes
#         connection.commit()
#         print("Data loaded into Snowflake successfully")
#     except Exception as e:
#         print(f"Error loading data into Snowflake: {str(e)}")
#     finally:
#         cursor.close()
#         connection.close()
        
# load_data_task = PythonOperator(
#     task_id='load_data',
#     python_callable=load_data,
#     dag=dag_1,
# )

# # Set task dependencies
# read_data_task >> load_data_task





# Task 2: Load data into Snowflake using SnowflakeHook
# def load_data():
#     ti = kwargs['ti']
#     data = ti.xcom_pull(task_ids='read_data')
#     snowflake_hook = SnowflakeHook(snowflake_conn_id='snow_sc') 
#     connection = snowflake_hook.get_conn()
#     cursor = connection.cursor()

#     try:
       
#         database_name = "demo"
#         schema_name = "sc1"
#         table_name = "stage_harsha"
        

#         df = pd.read_csv(io.StringIO(data))  
#         records = df.values.tolist()

#         # Truncate the table before inserting new data (optional)
#         cursor.execute(f"TRUNCATE TABLE {database_name}.{schema_name}.{table_name}")

#         # Use COPY INTO to load data into Snowflake efficiently
#         cursor.executemany(f"INSERT INTO {database_name}.{schema_name}.{table_name} (column1, column2) VALUES (?, ?)", records)

#         # Commit the changes
#         connection.commit()
#         print("Data loaded into Snowflake successfully")
#     except Exception as e:
#         print(f"Error loading data into Snowflake: {str(e)}")
#     finally:
#         cursor.close()
#         connection.close()

# load_data_task = PythonOperator(
#     task_id='load_data',
#     python_callable=load_data,
#     dag=dag_1,
# )

# # Set task dependencies
# read_data_task >> load_data_task





# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow import DAG
# from datetime import datetime
# import requests

# default_args = {
#     'start_date': datetime(2023, 8, 31),
#     'catchup': False,
# }

# dag_1 = DAG(
#     'dag_1_h',
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
# )

# # Create a function to load data into Snowflake
# def load_data_to_snowflake():
#     url = "https://raw.githubusercontent.com/cs109/2014_data/master/countries.csv"
#     response = requests.get(url)
#     print(f"Status code: {response.status_code}")
#     print(f"Response content: {response.text}")
    
#     if response.status_code == 200:
#         data = response.text
#         print("Fetched data from URL:")
#         print(data)  
        
#         lines = data.strip().split('\n')[1:]
        
#         if not lines:
#             raise ValueError("No data found in the CSV file.")
        
#         # Construct SQL statements
#         sql_statements = [
#             f"INSERT INTO temp_harsha (country, region) VALUES ('{line.split(',')[0]}', '{line.split(',')[1]}')"
#             for line in lines
#         ]
        
#         if not sql_statements:
#             raise ValueError("No SQL statements generated.")
        
#         # Assuming you have defined Snowflake connection properly
#         snowflake_task = SnowflakeOperator(
#             task_id='load_data_task',
#             sql=sql_statements,
#             snowflake_conn_id="snowflake_conn",
#             autocommit=True,  # Set to True if autocommit is enabled in Snowflake
#             dag=dag_1,
#         )
#         snowflake_task.execute(context={})  # Execute the Snowflake task
#     else:
#         raise Exception(f"Failed to fetch data from URL. Status code: {response.status_code}")

# # Create the PythonOperator task
# load_data_task = PythonOperator(
#     task_id='fetch_and_load_data',
#     python_callable=load_data_to_snowflake,
#     dag=dag_1,
# )

# load_data_task



# task_1 >> load_data_into_snowflake_task
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from airflow import DAG
# from datetime import datetime
# import requests

# default_args = {
#     'start_date': datetime(2023, 8, 31),
#     'catchup': False,
# }

# dag_1 = DAG(
#     'dag_1_h',
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
# )

# def read_file_from_url():
#     url = 'https://raw.githubusercontent.com/cs109/2014_data/master/countries.csv'
#     response = requests.get(url)
#     if response.status_code == 200:
#         return response.text
#     else:
#         raise Exception(f"Failed to retrieve data from URL: {url}. Status code: {response.status_code}")

# task_1 = PythonOperator(
#     task_id='read_file_from_url',
#     python_callable=read_file_from_url,
#     dag=dag_1,
# )

# def load_data_into_snowflake(**kwargs):
#     ti = kwargs['ti']
#     response_text = ti.xcom_pull(task_ids='read_file_from_url')
    
#     # Split the response_text and insert into Snowflake
#     for line in response_text.strip().split('\n')[1:]:
#         values = line.split(',')
#         query = f"""
#             INSERT INTO exusi_schema.temp_harsha (Country, Region)
#             VALUES ('{values[0]}', '{values[1]}')
#         """
        
#         # Execute the query in Snowflake
#         snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
#         snowflake_hook.run(query)
#         print("Data loaded into Snowflake successfully.")
    
#     except Exception as e:
#         # Handle the exception
#         print(f"Error loading data into Snowflake: {str(e)}")
#         raise

# load_data_into_snowflake_task = PythonOperator(
#     task_id="load_data_into_snowflake",
#     python_callable=load_data_into_snowflake,
#     provide_context=True,
#     dag=dag_1,
# )

# check_load_success = SnowflakeOperator(
#     task_id='check_load_success',
#     sql="SELECT COUNT(*) FROM temp_harsha",  
#     snowflake_conn_id="snowflake_conn_id",  
#     # mode='reschedule',
#     # timeout=3600,
#     # poke_interval=60,
#     dag=dag_1,
# )

# trigger_dag_2 = TriggerDagRunOperator(
#     task_id='trigger_dag_2',
#     trigger_dag_id="dag_2_h",
#     dag=dag_1,
#     trigger_rule="all_success",
# )

# task_1 >> load_data_into_snowflake_task >> check_load_success >> trigger_dag_2



# import requests
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from datetime import datetime
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# # Default arguments for the DAG
# default_args = {
#     'start_date': datetime(2023, 8, 31),
#     'catchup': False,
# }

# dag = DAG(
#     'dag_1_h',
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
# )

# def read_file_from_url(**kwargs):
#     url = 'https://raw.githubusercontent.com/cs109/2014_data/master/countries.csv'
#     response = requests.get(url)
#     if response.status_code == 200:
#         return response.text
#     else:
#         raise Exception(f"Failed to retrieve data from URL: {url}. Status code: {response.status_code}")

# task_1 = PythonOperator(
#     task_id='read_file_from_url',
#     python_callable=read_file_from_url,
#     provide_context=True,
#     dag=dag,
# )

# def load_data_temp_table(**kwargs):
#     response_content = kwargs['ti'].xcom_pull(task_ids='read_file_from_url')
#     lines = response_content.strip().split('\n')[1:]
#     # snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
#     snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
#     snowflake_hook.get_conn()

        
#     for line in lines:
#         values = line.split(',')
#         query = f"""
#             INSERT INTO temp_harsha (Country, Region)
#             VALUES ('{values[0]}', '{values[1]}')
#             """
#         # snowflake_hook.run(query)
#         # print("Data loaded into Snowflake successfully.")
#         try:
#            snowflake_hook.run(query)
#            print("Data loaded into Snowflake successfully.")
#         except Exception as e:
#           print(f"Error loading data into Snowflake: {str(e)}")
#           raise


# # Add a task to check if the load is successful or not
# def check_load_success(**kwargs):
#     # Assuming you have some criteria to check the success of the load
#     # For example, check if records were inserted successfully
#     successful_load = True  # Replace with your own logic
#     if successful_load:
#         return 'success'
#     else:
#         return 'failure'

# task_2 = PythonOperator(
#     task_id='load_data_stage_table',
#     python_callable=load_data_temp_table,
#     provide_context=True,
#     dag=dag,
# )

# task_3 = PythonOperator(
#     task_id='check_load_success',
#     python_callable=check_load_success,
#     provide_context=True,
#     dag=dag,
# )

# # Define DAG 2 (dag_2_h) here, replace with your actual DAG definition
# dag_2 = DAG(
#     'dag_2_h',
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
# )

# # Define tasks in DAG 2 here

# # TriggerDagRunOperator to trigger dag_2_h
# trigger_dag_2 = TriggerDagRunOperator(
#     task_id='trigger_dag_2',
#     trigger_dag_id="dag_2_h",  # Trigger dag_2_h
#     dag=dag,
# )

# # Define the task dependencies in dag_1_h
# task_1 >> task_2 >> task_3 >> trigger_dag_2








# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.operators.python import ShortCircuitOperator
# from airflow.utils.dates import days_ago
# from airflow.models import Variable
# import os
# import requests

# default_args = {
#     'owner': 'airflow',
#     'start_date': days_ago(1),
#     'catchup': False,
#     'provide_context': True,
# }

# dag = DAG(
#     'harsha_dag',
#     default_args=default_args,
#     schedule_interval=None,
# )


# def check_environment_variable():
#     # variable_value = 'harsha_air_env'
#     # variable_value = Variable.get('harsha_air_env')
#     # return variable_value == "True"
#     Variable.set("my_boolean_variable", False)
#     bool = Variable.get('my_boolean_variable')
#     if bool :
#         print( bool )
#         return True
#     # if Variable.get('harsha_air_env').lower() == 'true':
#     #     return 'load_data_to_snowflake'
#     else:
#         #stop dag
#         return False
        
     
# task_1 = ShortCircuitOperator(
#     task_id='check_env_variable',
#     python_callable=check_environment_variable,
#     provide_context=True,
#     dag=dag,
# )

# def check_environment_variable():
#     variable_value = Variable.get('harsha_air_env', default_var=None)
#     if variable_value is not None:
#         return variable_value.lower() == True
#     else:
#         # Stop dag
#         return False

# task_1 = ShortCircuitOperator(
#     task_id='check_env_variable',
#     python_callable=check_environment_variable,
#     provide_context=True,
#     dag=dag,
# )




# the fucntion is loding the data from url to snowflake
# def load_data_to_snowflake(**kwargs):
#     url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
#     response = requests.get(url)
    
#     if response.status_code == 200:
#         data = response.text
#         lines = data.strip().split('\n')[1:]
#         snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        
#         for line in lines:
#             values = line.split(',')
#             query = f"""
#                 INSERT INTO airflow_tasks (airline, avail_seat_km_per_week, incidents_85_99, fatal_accidents_85_99, fatalities_85_99, incidents_00_14, fatal_accidents_00_14, fatalities_00_14)
#                 VALUES ('{values[0]}', '{values[1]}', '{values[2]}', '{values[3]}', '{values[4]}', '{values[5]}', '{values[6]}', '{values[7]}')
#             """
#             snowflake_hook.run(query)
            
#         print("Data loaded into Snowflake successfully.")
#     else:
#         raise Exception(f"Failed to fetch data from URL. Status code: {response.status_code}")

# task_2 = PythonOperator(
#     task_id='load_data_task',
#     python_callable=load_data_to_snowflake,
#     provide_context=True,
#     dag=dag,
# )

# # the function is getting records from table graterthan 698012498

# def print_records_all(**kwargs):
#     snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
#     query = """ SELECT * FROM airflow_tasks WHERE avail_seat_km_per_week > 698012498 
#      """
#     records = snowflake_hook.get_records(query)
#     print("Printing records:")
#     for record in records:
#         print(record)

# task_3 = PythonOperator(
#     task_id='print_all_records_task',
#     python_callable=print_records_all,
#     provide_context=True,
#     dag=dag,
# )
# # function working on condition 
# def print_records_limit(**kwargs):
#     snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
#     query = "SELECT * FROM airflow_tasks WHERE avail_seat_km_per_week > 698012498 LIMIT 10"
#     records = snowflake_hook.get_records(query)
    
#     if records:
#         print("Printing 10 records:")
#     else:
#         query = "SELECT * FROM airflow_tasks LIMIT 5"
#         records = snowflake_hook.get_records(query)
#         print("Printing 5 records:")
    
#     for record in records:
#         print(record)

# task_4 = PythonOperator(
#     task_id='print_limit_records_task',
#     python_callable=print_records_limit,
#     provide_context=True,
#     dag=dag,
# )

# def print_completed(**kwargs):
#     print("Process completed.")

# task_5 = PythonOperator(
#     task_id='print_completed_task',
#     python_callable=print_completed,
#     provide_context=True,
#     dag=dag,
# )

# task_1 >> task_2 >> task_3 >> task_4 >> task_5

# /==========================================================================================
# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from datetime import datetime

# default_args = {
#      'owner': 'airflow',
#      'start_date': datetime(2023, 1, 1),
#      'retries': 1,
#  }

# dag = DAG(
#      'harsha_dag',  
#      default_args=default_args,
#      schedule_interval=None,
#      catchup=False,
#  )

# sql_query = """
#  SELECT * FROM airflow_tasks
# WHERE avail_seat_km_per_week > 698012498
# """

# snowflake_task = SnowflakeOperator(
#      task_id='execute_snowflake_query',
#      sql=sql_query,
#      snowflake_conn_id='snowflake_conn',
#      autocommit=True,
#      dag=dag,
#  )




# =======================================working==============================
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
