from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'airflow_dag',  
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

# Task 3: Check if avail_seat_km_per_week is greater than 698012498
def check_seat_km():
    # Replace this with your actual Snowflake query
    sql_query = "SELECT COUNT(*) FROM airflow_tasks WHERE avail_seat_km_per_week > 698012498"
    return sql_query

check_seat_task = SnowflakeOperator(
    task_id='check_seat_task',
    sql=check_seat_km,
    snowflake_conn_id='snowflake_conn',
    autocommit=True,
    dag=dag,
)

# Task 4: Print 10 or 5 records based on the previous task
def print_records(**kwargs):
    task_instance = kwargs['ti']
    task_result = task_instance.xcom_pull(task_ids='check_seat_task')
    record_count = task_result[0][0]  # Get the count from the first row

    if record_count > 0:
        sql_query = "SELECT * FROM airflow_tasks LIMIT 10"
    else:
        sql_query = "SELECT * FROM airflow_tasks LIMIT 5"
    
    snowflake_task = SnowflakeOperator(
        task_id='print_records_task',
        sql=sql_query,
        snowflake_conn_id='snowflake_conn',  
        autocommit=True,
        dag=dag,
    )

    snowflake_task.execute(context=kwargs)

print_records_task = PythonOperator(
    task_id='print_records_task',
    python_callable=print_records,
    provide_context=True,
    dag=dag,
)

# Task 5: Print "Process completed"
def print_completed(**kwargs):
    print("Process completed")

print_completed_task = PythonOperator(
    task_id='print_completed_task',
    python_callable=print_completed,
    provide_context=True,
    dag=dag,
)

check_seat_task >> print_records_task >> print_completed_task



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
#     'provide_context': True,
# }

# dag = DAG(
#     'airline_safety_dag',
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



# # Task 3: Check if avail_seat_km_per_week is greater than 698012498
# def check_avail_seat_km(**kwargs):
#     snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
#     query = """
#     SELECT COUNT(*) as record_count FROM airflow_tasks WHERE avail_seat_km_per_week > 698012498
#     """
#     result = snowflake_hook.get_first(query)
#     record_count = result[0]
    
#     if record_count > 0:
#         return 'print_10_records_task'
#     else:
#         return 'print_5_records_task'

# task_3 = PythonOperator(
#     task_id='check_avail_seat_km',
#     python_callable=check_avail_seat_km,
#     provide_context=True,
#     dag=dag,
# )

# task_1 >> task_3



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
