from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests
from io import StringIO
import pandas as pd

# Snowflake connection ID
SNOWFLAKE_CONN_ID = 'snow_sc'

default_args = {
    'start_date': datetime(2023, 8, 25),
    'retries': 1,
    'catchup': True,
}

dag_args = {
    'dag_id': 'charishma_csv_dag',
    'schedule_interval': None,
    'default_args': default_args,
    'catchup': False,
}
dag = DAG(**dag_args)

def read_file_from_url():
    url = "https://raw.githubusercontent.com/jcharishma/my.repo/master/sample_csv.csv"
    response = requests.get(url)
    data = response.text
    print(f"Read data from URL. Content: {data}")
    return data

def load_data_to_snowflake(**kwargs):
    data = kwargs['task_instance'].xcom_pull(task_ids='read_file_task')
    df = pd.read_csv(StringIO(data))
    
    # Define Snowflake table names
    main_table = 'sample_csv'
    error_table = 'error_log'
    
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
    # Split data based on SSN criteria
    valid_data = df[df['SSN'].str.len() == 4]
    invalid_data = df[df['SSN'].str.len() != 4]
    
    # Load valid data to main table
    if not valid_data.empty:
        snowflake_hook.insert_rows(main_table, valid_data.values.tolist(), valid_data.columns.tolist())
    
    # Load invalid data to error_log table
    if not invalid_data.empty:
        invalid_data['Error_message'] = 'Invalid SSN length'
        snowflake_hook.insert_rows(error_table, invalid_data.values.tolist(), invalid_data.columns.tolist())

with dag:
    read_file_task = PythonOperator(
        task_id='read_file_task',
        python_callable=read_file_from_url,
    )

    load_to_snowflake_task = PythonOperator(
        task_id='load_to_snowflake_task',
        python_callable=load_data_to_snowflake,
    )

    read_file_task >> load_to_snowflake_task










# from airflow import DAG
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.operators.dummy import DummyOperator 
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from datetime import datetime, timedelta
# import requests
# from io import StringIO
# import pandas as pd
# import logging

# # Snowflake connection ID
# SNOWFLAKE_CONN_ID = 'snow_sc'

# default_args = {
#     'start_date': datetime(2023, 8, 25),
#     'retries': 1,
#     'catchup': True, 
# }

# dag_args = {
#     'dag_id': 'charishma_dags',
#     'schedule_interval': None,
#     'default_args': default_args,
#     'catchup': False,  
# }
# dag = DAG(**dag_args)



# def read_file_from_url():
#     url = "https://raw.githubusercontent.com/cs109/2014_data/master/countries.csv"
#     response = requests.get(url)
#     return response.text

# def load_data_to_staging(data):
#     df = pd.read_csv(StringIO(data))
#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
#     table_name = 'demo.sc1.stage_table'
#     snowflake_hook.insert_rows(table_name, df.values.tolist(), df.columns.tolist())

# def check_load_success(**kwargs):
#     return True  # Placeholder for your actual success condition

# with dag:
#     read_file_task = PythonOperator(
#         task_id='read_file_task',
#         python_callable=read_file_from_url,
#     )

#     load_to_staging_task = PythonOperator(
#         task_id='load_to_staging_task',
#         python_callable=load_data_to_staging,
#         op_args=[read_file_task.output],
#     )

#     check_load_task = PythonOperator(
#         task_id='check_load_task',
#         python_callable=check_load_success,
#     )

#     trigger_dag2_task = TriggerDagRunOperator(
#         task_id='trigger_dag2_task',
#         trigger_dag_id='charishma_dag2',
#     )

#     read_file_task >> load_to_staging_task >> check_load_task >> trigger_dag2_task

# dag2 = DAG(dag_id='charishma_dag2', default_args=default_args, schedule_interval=None, catchup=False)

# def load_data_to_main(**kwargs):
#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
#     connection = snowflake_hook.get_conn()
    
#     try:
        
#         insert_query = "INSERT INTO main_table SELECT * FROM stage_table;"
#         cursor = connection.cursor()
#         cursor.execute(insert_query)
#         connection.commit()
#         cursor.close()
#         connection.close()
#         return True
#     except Exception as e:
#         logging.error(f"Error loading data to main table: {str(e)}")
#         return False
# def check_load_main_success(**kwargs):
  
#     return True  


# def print_status(**kwargs):
#     logging.info("Process Completed")

# with dag2:
#     load_main_task = PythonOperator(
#         task_id='load_main_task',
#         python_callable=load_data_to_main,
#     )

#     check_load_main_task = PythonOperator(
#         task_id='check_load_main_task',
#         python_callable=check_load_main_success,
#     )

#     print_status_task = PythonOperator(
#         task_id='print_status_task',
#         python_callable=print_status,
#     )

#     load_main_task >> check_load_main_task >> print_status_task










# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from datetime import datetime
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# import requests
# from io import StringIO
# import pandas as pd

# # Snowflake connection ID
# SNOWFLAKE_CONN_ID = 'snow_sc'

# default_args = {
#     'start_date': datetime(2023, 8, 25),
#     'retries': 1,
# }

# # Define the DAG
# with DAG('charishma_dags', schedule_interval=None, default_args=default_args) as dag:
#     def read_file_from_url():
#         url = "https://raw.githubusercontent.com/cs109/2014_data/master/countries.csv"
#         response = requests.get(url)
#         return response.text

#     # Task 1: Read the file from URL
#     read_file_task = PythonOperator(
#         task_id='read_file_task',
#         python_callable=read_file_from_url,
#     )

#     def load_data_to_staging(data):
#         df = pd.read_csv(StringIO(data))
#         snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
#         table_name = 'demo.sc1.stage_table'  
#         snowflake_hook.insert_rows(table_name, df.values.tolist(), df.columns.tolist())

#     # Task 2: Load into the Snowflake DB staging table
#     load_to_staging_task = PythonOperator(
#         task_id='load_to_staging_task',
#         python_callable=load_data_to_staging,
#         op_args=[read_file_task.output],
#     )

#     # Task 3: Check if the load is successful 
#     def trigger_dag2(**kwargs):
#         ti = kwargs['ti']
#         load_successful = ti.xcom_pull(task_ids='check_load_task')
#         if load_successful:
#             # Trigger DAG 2
#             trigger_dag2task = TriggerDagRunOperator(
#                 task_id='trigger_dag2',
#                 trigger_dag_id='charishma_dag2',
#                 dag=dag,
#             )
#             return trigger_dag2task

#     trigger_dag2_task = PythonOperator(
#         task_id='trigger_dag2_task',
#         python_callable=trigger_dag2,
#         provide_context=True,
#     )

#     read_file_task >> load_to_staging_task >> trigger_dag2_task


# # Define DAG 2
# with DAG('charishma_dag2', schedule_interval=None, default_args=default_args) as dag2:
#     def load_data_to_main():
#         snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

#         sql = '''
#             INSERT INTO demo.sc1.main_table
#             SELECT * FROM demo.sc1.stage_table;
#         '''
#         snowflake_hook.run(sql)

#     # Task 1: Load the data from staging table to main
#     load_to_main = PythonOperator(
#         task_id='load_to_main',
#         python_callable=load_data_to_main,
#     )

#     def check_load_success():
#         snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
#         # Implementing logic to check if the load was successful count the rows  tables
#         staging_count_query = "SELECT COUNT(*) FROM stage_table;"
#         main_count_query = "SELECT COUNT(*) FROM main_table;"

#         staging_count = snowflake_hook.get_first(staging_count_query)[0]
#         main_count = snowflake_hook.get_first(main_count_query)[0]

#         # Compare the row counts to determine if the load was successful
#         if staging_count == main_count:
#             return True
#         else:
#             return False

#     # Task 2: Check if load is successful or not
#     check_load_task = PythonOperator(
#         task_id='check_load_task',
#         python_callable=check_load_success,
#     )

#     def print_status(load_success):
#         if load_success:
#             print("Load was successful!")
#         else:
#             print("Load failed!")

#     # Task 3: Print success or failure status from T-2 task
#     print_status_task = PythonOperator(
#         task_id='print_status_task',
#         python_callable=print_status,
#         op_args=[check_load_task.output],
#     )

#     # Set up task dependencies
#     load_to_main >> check_load_task >> print_status_task
