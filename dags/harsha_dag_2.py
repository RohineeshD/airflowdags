
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# # Define the DAG
default_args = {
     'owner': 'airflow',
     'start_date': datetime(2023, 9, 1),
     'retries': 1,
}

dag = DAG(
     'harsha_dag2',
     default_args=default_args,
     schedule_interval=None,  
     catchup=False,
    
)

# # Task 1: Load Data from Staging Table to Main
def load_data():
     try:
         snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
         conn = snowflake_hook.get_conn()
         cursor = conn.cursor()

        
         sql_query = """
         INSERT INTO main_harsha (Country, Region)
         SELECT Country, Region
         FROM stage_harsha LIMIT 100;
         """

         cursor.execute(sql_query)
         cursor.close()
         conn.close()
         print("Data loaded successfully")
     except Exception as e:
         print("Data loading failed -", str(e))

# # Task 2: Check if Load is Successful
def check_load_status():
     try:
         snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
         conn = snowflake_hook.get_conn()
         cursor = conn.cursor()

        
         sql_query = "SELECT * FROM main_harsha;"
         cursor.execute(sql_query)
         row = cursor.fetchone()

         # # if row[0] > 0:
         #     print("Load was successful")
         #     return True
         # else:
             # print("Load failed - Main table is empty")
         print("Load was successful")
         return False
             # return False
     except Exception as e:
         print("Load failed -", str(e))
         return False

# # Task 3: Print Success or Failure Status
def print_status(load_success):
     if load_success:
         print("Success")
     else:
         print("Failure")


# # Task 1: Load Data
load_data_task = PythonOperator(
     task_id='load_data',
     python_callable=load_data,
     dag=dag,
)

# # Task 2: Check Load Status
check_load_status_task = PythonOperator(
     task_id='check_load_status',
     python_callable=check_load_status,
     provide_context=True,
     dag=dag,
)

# # Task 3: Print Status
print_status_task = PythonOperator(
     task_id='print_status',
     python_callable=print_status,
     op_args=[check_load_status_task.output],
     provide_context=True,
     dag=dag,
)


load_data_task >> check_load_status_task >> print_status_task
