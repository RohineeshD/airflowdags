# Importing necessary modules
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

# Define the DAG's default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Create the DAG object
dag = DAG(
    'charishma_dag',
    default_args=default_args,
    schedule_interval=None,  # Set the schedule interval as needed
    catchup=False,
)

# Define the SQL query you want to execute in Snowflake
sql_query = """
SELECT * FROM table1
"""

# Create a SnowflakeOperator task
snowflake_task = SnowflakeOperator(
    task_id='execute_snowflake_query',
    sql=sql_query,
    snowflake_conn_id='snowflake_connection',  # Set this to your Snowflake connection ID
    autocommit=True,  # Set autocommit to True if needed
    dag=dag,
)

# Set task dependencies
snowflake_task




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
