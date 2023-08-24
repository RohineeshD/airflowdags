from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime



# Step 2: Initiating the default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 12),
}

# Define a Python function to be executed by the PythonOperator
def print_hello():
    print("Welcome to Charishma's dag!")

# Step 3: Creating DAG Object
dag = DAG(
    dag_id='charishma_dag',
    default_args=default_args,
    schedule_interval='@once',  
    catchup=False,
)

# Step 4: Creating task
# Create a PythonOperator that will run the print_hello function
task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)


# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from datetime import datetime

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 1, 1),
#     'retries': 1,
# }

# dag = DAG(
#     'charishma_dags',  # Change the dag_id to a unique name
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
# )

# sql_query = """
# SELECT SUM(id) AS total_id_sum
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
