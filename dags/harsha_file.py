# from airflow import DAG
# from airflow.providers.http.sensors.http import HttpSensor
# from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta
# import pandas as pd
# import requests
# import tempfile

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 8, 25),
#     'retries': 1,
# }

# dag = DAG(
#     'airline_safety_dag',
#     default_args=default_args,
#     schedule_interval='@once',  # Set to None for manual triggering
#     catchup=False,
# )

# def check_env_variable(**kwargs):
#     if kwargs['dag_run'].conf.get('load_data'):
#         return 'load_data_task'

# check_env_task = PythonOperator(
#     task_id='check_env_task',
#     python_callable=check_env_variable,
#     provide_context=True,
#     dag=dag,
# )

# def read_data_and_load_to_snowflake(**kwargs):
#     url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
#     response = requests.get(url)
#     data = response.text
    
#     # Create a temporary file to store the data
#     with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
#         temp_file.write(data.encode())
#         temp_file_path = temp_file.name

#     # Load data into Snowflake using SnowflakeOperator
#     sql_query = f"""
#     COPY INTO airflow_tasks
#     FROM '{temp_file_path}'
#     FILE_FORMAT = (TYPE = CSV)
#     """
    
#     snowflake_task = SnowflakeOperator(
#         task_id='load_data_into_snowflake',
#         sql=sql_query,
#         snowflake_conn_id='snowflake_conn',  # Specify your Snowflake connection ID
#         autocommit=True,
#         dag=dag,
#     )
    
#     snowflake_task.execute(context=kwargs)

# load_data_task = PythonOperator(
#     task_id='load_data_task',
#     python_callable=read_data_and_load_to_snowflake,
#     provide_context=True,
#     dag=dag,
# )

# check_env_task >> load_data_task


from airflow import DAG
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import tempfile
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 25),
    'retries': 1,
}

dag = DAG(
    'airline_safety_dag_harsha1',
    default_args=default_args,
    schedule_interval='@once',  # Set to None for manual triggering
    catchup=False,
)

def check_env_variable(**kwargs):
    if kwargs['dag_run'].conf.get('load_data'):
        return 'load_data_task'

check_env_task1 = PythonOperator(
    task_id='check_env_task',
    python_callable=check_env_variable,
    provide_context=True,
    dag=dag,
)

def read_data_and_load_to_snowflake(**kwargs):
    url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for non-200 responses
        data = response.text

        # Create a temporary file to store the data
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
            temp_file.write(data.encode())
            temp_file_path = temp_file.name

        # Load data into Snowflake
        # Specify your Snowflake connection ID and other parameters
        snowflake_conn_id = 'snowflake_conn'
        table_name = 'airflow_tasks'
        schema_name = 'exusia_schema'

        load_query = f"""
        COPY INTO {schema_name}.{table_name}
        FROM '{temp_file_path}'
        FILE_FORMAT = (TYPE = CSV)
        """
        
        # Execute the query using the SnowflakeHook
        snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        snowflake_hook.run(load_query)
        
        logging.info("Data loaded into Snowflake successfully.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data from URL: {e}")

load_data_task2 = PythonOperator(
    task_id='load_data_task',
    python_callable=read_data_and_load_to_snowflake,
    provide_context=True,
    dag=dag,
)

check_env_task1 >> load_data_task2


# from airflow import DAG
# from airflow.providers.http.sensors.http import HttpSensor
# from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta
# import pandas as pd
# import requests
# import tempfile
# import logging

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2022, 11, 12),
#     'retries': 1,
# }

# dag = DAG(
#     'airline_safety_dag_harsha',
#     default_args=default_args,
#     schedule_interval='@once',  # Set to None for manual triggering
#     catchup=False,
# )

# def check_env_variable(**kwargs):
#     if kwargs['dag_run'].conf.get('load_data'):
#         return 'load_data_task'

# check_env_task = PythonOperator(
#     task_id='check_env_task',
#     python_callable=check_env_variable,
#     provide_context=True,
#     dag=dag,
# )

# def read_data_and_load_to_snowflake(**kwargs):
#     url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
    
#     try:
#         response = requests.get(url)
#         response.raise_for_status()  # Raise exception for non-200 responses
#         data = response.text

#         # Create a temporary file to store the data
#         with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
#             temp_file.write(data.encode())
#             temp_file_path = temp_file.name

#         # Load data into Snowflake using SnowflakeOperator
#         sql_query = f"""
#         COPY INTO airflow_tasks
#         FROM '{temp_file_path}'
#         FILE_FORMAT = (TYPE = CSV)
#         """

#         snowflake_task = SnowflakeOperator(
#             task_id='load_data_into_snowflake',
#             sql=sql_query,
#             snowflake_conn_id='snowflake_conn',  # Specify your Snowflake connection ID
#             autocommit=True,
#             dag=dag,
#         )

#         snowflake_task.execute(context=kwargs)
#         logging.info("Data loaded into Snowflake successfully.")
#     except requests.exceptions.RequestException as e:
#         logging.error(f"Failed to fetch data from URL: {e}")

# load_data_task = PythonOperator(
#     task_id='load_data_task',
#     python_callable=read_data_and_load_to_snowflake,
#     provide_context=True,
#     dag=dag,
# )

# check_env_task >> load_data_task



# from airflow import DAG
# from airflow.providers.http.sensors.http import HttpSensor
# from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta
# import pandas as pd
# import requests
# import tempfile

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 8, 25),
#     'retries': 1,
# }

# dag = DAG(
#     'airline_safety_dag',
#     default_args=default_args,
#     schedule_interval='@once',  # Set to None for manual triggering
#     catchup=False,
# )

# def check_env_variable(**kwargs):
#     if kwargs['dag_run'].conf.get('load_data'):
#         return 'load_data_task'

# check_env_task = PythonOperator(
#     task_id='check_env_task',
#     python_callable=check_env_variable,
#     provide_context=True,
#     dag=dag,
# )

# def read_data_and_load_to_snowflake(**kwargs):
#     url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
#     response = requests.get(url)
#     data = response.text
    
#     # Create a temporary file to store the data
#     with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
#         temp_file.write(data.encode())
#         temp_file_path = temp_file.name

#     # Load data into Snowflake using SnowflakeOperator
#     sql_query = f"""
#     COPY INTO airflow_tasks
#     FROM '{temp_file_path}'
#     FILE_FORMAT = (TYPE = CSV)
#     """
    
#     snowflake_task = SnowflakeOperator(
#         task_id='load_data_into_snowflake',
#         sql=sql_query,
#         snowflake_conn_id='snowflake_conn',  # Specify your Snowflake connection ID
#         autocommit=True,
#         dag=dag,
#     )
    
#     snowflake_task.execute(context=kwargs)

# load_data_task = PythonOperator(
#     task_id='load_data_task',
#     python_callable=read_data_and_load_to_snowflake,
#     provide_context=True,
#     dag=dag,
# )

# check_env_task >> load_data_task


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
# "SELECT * FROM patients WHERE status = 'Recovered'
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
