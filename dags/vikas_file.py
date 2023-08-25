# Step 1: Importing Modules
# To initiate the DAG Object
from airflow import DAG
# Importing datetime and timedelta modules for scheduling the DAGs
from datetime import timedelta, datetime
# Importing operators
from airflow.operators.http_sensor import HttpSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import requests

# Step 2: Initiating the default_args
default_args = {
        'owner' : 'airflow',
        'start_date' : datetime(2023, 8, 25),

}

# Step 3: Creating DAG Object
# dag = DAG(dag_id='vikas_dag',
#         default_args=default_args,
#         schedule_interval='@once',
#         catchup=False
#     )

dag = DAG('read_url_data', default_args=default_args, schedule_interval=None)


def read_data_from_url(**kwargs):
        url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
        response = requests.get(url)
        data = response.text

        # Do something with the data, like processing or storing it
        # For now, let's just print it
        print(data)

read_data_task = PythonOperator(
    task_id='read_data_task',
    python_callable=read_data_from_url,
    provide_context=True,
    dag=dag,
)

url_sensor = HttpSensor(
    task_id='url_sensor',
    http_conn_id='http_default',  # This should be set up in Airflow connections
    endpoint='YOUR_URL_ENDPOINT',
    request_params={},  # Additional request parameters if needed
    dag=dag,
)

url_sensor >> read_data_task





























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
# dag = DAG(dag_id='vikas_dag',
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
