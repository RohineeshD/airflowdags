from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
import os

default_args = {
    'start_date': datetime(2023, 8, 25),
    'retries': 1,
}

def check_env_variable(**kwargs):
    c_air_env = os.environ.get('C_AIR_ENV')
    print(f"Value of C_AIR_ENV: {c_air_env}")
    if c_air_env == 'true':
        return 'load_data_task'
    return None

with DAG('charishma_dags', schedule_interval=None, default_args=default_args) as dag:
    check_env_task = BranchPythonOperator(
        task_id='check_env_variable',
        python_callable=check_env_variable,
        provide_context=True,
    )

    load_data_task = SnowflakeOperator(
        task_id='load_data_task',
        sql=f"COPY INTO airflow_tasks "
            f"FROM 'https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv' "
            f"FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);",
        snowflake_conn_id='snowflake',
    )

    check_env_task >> load_data_task


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


from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'charishma_dags',  
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
)

sql_query = """
SELECT max(id) AS max_id
FROM table1
"""

snowflake_task = SnowflakeOperator(
    task_id='execute_snowflake_query',
    sql=sql_query,
    snowflake_conn_id='snow_conn',
    autocommit=True,
    dag=dag,
)




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
