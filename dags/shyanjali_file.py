from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'shyanjali_dag',  # Change the dag_id to a unique name
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
)

sql_query = """
SELECT SUM(id) AS total_id_sum
FROM table1
"""

snowflake_task = SnowflakeOperator(
    task_id='execute_snowflake_query',
    sql=sql_query,
    snowflake_conn_id='snow_conn',
    autocommit=True,
    dag=dag,
)

# -----------------------------
# from datetime import datetime
# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python_operator import PythonOperator

# def print_hello():
#     return 'Hello world from first Airflow DAG!'

# dag = DAG('shyanjali_dag',
#           schedule_interval='@once',
#           start_date=datetime(2022, 11, 12), catchup=False)

# hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

# hello_operator
# ---------------------------------
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
# dag = DAG(dag_id='shyanjali_dag',
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
