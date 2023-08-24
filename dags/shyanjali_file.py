try:
    import logging
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import os
    from io import StringIO
    import pandas as pd
    import requests
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    from airflow.providers.snowflake.transfers.s3_to_snowflake import SnowflakeOperator



    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_environment_variable():
    env_variable_value = os.environ.get('AIRFLOW__WEBSERVER__AUTHENTICATE')  # Replace with your actual environment variable name
    return env_variable_value == "True"


def fetch_csv_and_upload(**kwargs):
    url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
    response = requests.get(url)
    data = response.text
    df = pd.read_csv(StringIO(data))
    
    
    # Upload DataFrame to Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_li')
    
    # Replace with your Snowflake schema and table name
    schema = 'PUBLIC'
    table_name = 'AIRLINE'
    
    # Replace with your column names and data types
    # column_definitions = [
    #     "col1 VARCHAR",
    #     "col2 INTEGER",
    #     # Define more columns here
    # ]
    
    # create_table_query = f"""
    #     CREATE OR REPLACE TABLE {schema}.{table_name} (
    #         {', '.join(column_definitions)}
    #     )
    # """
    
    connection = snowflake_hook.get_conn()
    # cursor = connection.cursor()
    # cursor.execute(create_table_query)
    # cursor.close()
    
    snowflake_hook.insert_rows(table_name, df.values.tolist())
    
    connection.close()


def get_data(**kwargs):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_li')
    connection = snowflake_hook.get_conn()
    create_table_query="SELECT TOP (10) FROM AIRLINE WHERE avail_seat_km_per_week >698012498"
    cursor = connection.cursor()
    cursor.execute(create_table_query)
    for row in cursor.fetchall():
        # Process the retrieved data as needed
        logging.info(row)

    cursor.close()
    connection.close()
    
def print_success(**kwargs):
    logging.info("Process Completed")
    
# def get_all_env_variables(**kwargs):
#     env_variables = os.environ
#     for key, value in env_variables.items():
#         logging.info(f"Variable: {key}, Value: {value}")

with DAG(
        dag_id="shyanjali_dag",
        schedule_interval="@once",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        },
        catchup=False) as f:

    check_env_variable = PythonOperator(
    task_id='check_env_variable',
    python_callable=check_environment_variable,
    provide_context=True,
    )

    fetch_and_upload = PythonOperator(
        task_id='fetch_and_upload',
        python_callable=fetch_csv_and_upload,
        provide_context=True  # This is required to pass context to the function
    )

    get_data = PythonOperator(
        task_id='get_data',
        python_callable=get_data,
        provide_context=True  # This is required to pass context to the function
    )
    
    print_success = PythonOperator(
        task_id='print_success',
        python_callable=print_success,
        provide_context=True  # This is required to pass context to the function
    )


check_env_variable >> fetch_and_upload >>get_data>>print_success


# ------------------------
# try:
#     import logging
#     from datetime import timedelta
#     from airflow import DAG
#     from airflow.operators.python_operator import PythonOperator
#     from datetime import datetime
#     import pandas as pd
#     from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
#     from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

#     print("All Dag modules are ok ......")
# except Exception as e:
#     print("Error  {} ".format(e))

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
# def max_query(**context):
#     hook = SnowflakeHook(snowflake_conn_id="snowflake_li")
#     result = hook.get_first("select max(SSN) from public.emp_master")
#     logging.info("MAX", result[0])

# def count_query(**context):
#     hook = SnowflakeHook(snowflake_conn_id="snowflake_li")
#     result = hook.get_first("select count(*) from public.emp_master")
#     logging.info("COUNT", result[0])

# with DAG(
#         dag_id="shyanjali_dag",
#         schedule_interval="@once",
#         default_args={
#             "owner": "airflow",
#             "retries": 1,
#             "retry_delay": timedelta(minutes=5),
#             "start_date": datetime(2021, 1, 1),
#         },
#         catchup=False) as f:

#     query_table = PythonOperator(
#         task_id="max_query",
#         python_callable=max_query
#     )

#     query_table_1 = PythonOperator(
#         task_id="count_query",
#         python_callable=count_query
#     )

# query_table >> query_table_1

# ----------------------
# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from datetime import datetime

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 1, 1),
#     'retries': 1,
# }

# dag = DAG(
#     'shyanjali_dag',  # Change the dag_id to a unique name
#     default_args=default_args,
#     schedule_interval='@once',
#     catchup=False,
# )

# sql_query = """ SELECT * FROM public.emp_master """

# snowflake_task = SnowflakeOperator(
#     task_id='execute_snowflake_query_liconn',
#     sql=sql_query,
#     snowflake_conn_id='snowflake_li',
#     autocommit=True,
#     dag=dag,
# )

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
