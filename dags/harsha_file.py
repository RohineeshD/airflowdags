from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 12),
}

dag = DAG(
    dag_id='harsha_dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
)

# Retrieve data from Snowflake table using SnowflakeOperator
retrieve_data_task = SnowflakeOperator(
    task_id='retrieve_data_from_snowflake',
    sql="SELECT * FROM exusia_db.exusia_schema.patients WHERE  status = 'Recovered';",
    snowflake_conn_id='snowflake_connection',  # Specify your Snowflake connection ID
    autocommit=True,  # Auto-commit the transaction
    dag=dag,
)

# Define the order of tasks
retrieve_data_task

# Set task dependencies
retrieve_data_task


# ================================================================================================
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


# ===================================================================================
# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from datetime import datetime, timedelta
# from airflow.operators.dummy_operator import DummyOperator

# # Step 2: Initiating the default_args
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2022, 11, 12),
# }

# # Step 3: Creating DAG Object
# dag = DAG(
#     dag_id='harsha_dag',
#     default_args=default_args,
#     schedule_interval='@once',
#     catchup=False,
# )

# # Step 4: Creating tasks
# start = DummyOperator(task_id='start', dag=dag)

# # Retrieving the maximum patient ID using SnowflakeOperator
# retrieve_max_patient_id = SnowflakeOperator(
#     task_id='retrieve_max_patient_id',
#     sql="SELECT MAX(patient_id) FROM exusia_db.exusia_schema.patients;",
#     snowflake_conn_id='snowflake_conn',  # Specify your Snowflake connection ID
#     autocommit=True,  # Auto-commit the transaction
#     dag=dag,
# )

# # Creating second task
# end = DummyOperator(task_id='end', dag=dag)

# # Setting up dependencies
# start >> retrieve_max_patient_id >> end

# =======================================================================================================
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
# dag = DAG(dag_id='harsha_dag',
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
