from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

# Define your Snowflake connection ID from Airflow's Connection UI
snowflake_conn_id = "snowflake_conn"

# Define your DAG
dag = DAG(
    'csv_to_snowflake_example',
    start_date=datetime(2023, 9, 5),  # Adjust as needed
    schedule_interval=None,  # Set your desired schedule interval
    catchup=False  # Set to True if you want historical DAG runs to execute
)

# Define the URL of the CSV file
csv_url = 'https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv'

# Snowflake COPY INTO command
copy_into_sql = f'''
COPY INTO bulk_table
FROM '{csv_url}'
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
);
'''

# Task to execute the COPY INTO command using SnowflakeHook
copy_to_snowflake_task = SnowflakeOperator(
    task_id='copy_to_snowflake_task',
    sql=copy_into_sql,
    snowflake_conn_id=snowflake_conn_id,
    autocommit=True,  # Set to True for the COPY INTO operation
    dag=dag
)

if __name__ == "__main__":
    dag.cli()



# from datetime import datetime
# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# # # Define the DAG
# default_args = {
#      'owner': 'airflow',
#      'start_date': datetime(2023, 9, 1),
#      'retries': 1,
# }

# dag = DAG(
#      'harsh_dag',
#      default_args=default_args,
#      schedule_interval=None,  
#      catchup=False,
    
# )

# def load_data():
#     try:
#         snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
#         conn = snowflake_hook.get_conn()
#         cursor = conn.cursor()

#         # Truncate the main_table before loading data
#         truncate_query = """
#         TRUNCATE TABLE main_table;
#         """
#         cursor.execute(truncate_query)

#         # Insert data from stage_table into main_table
#         sql_query = """
#         INSERT INTO main_table (Country, Region)
#         SELECT Country, Region
#         FROM stage_table;
#         """
#         cursor.execute(sql_query)

#         cursor.close()
#         conn.close()
#         print("Data loaded successfully")
#         return True
#     except Exception as e:
#         print("Data loading failed -", str(e))
#         return False

# # # Task 2: Check if Load is Successful
# def check_load_status():
#      try:
#          snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
#          conn = snowflake_hook.get_conn()
#          cursor = conn.cursor()

        
#          sql_query = "SELECT * FROM main_table;"
#          cursor.execute(sql_query)
#          row = cursor.fetchall()

        
#          print("Load was successful")
#          return True
             
#      except Exception as e:
#          print("Load failed -", str(e))
#          return False

# # # Task 3: Print Success or Failure Status
# def print_status(load_success):
#      if load_success:
#          print("Success")
#      else:
#          print("Failure")


# # # Task 1: Load Data
# load_data_task = PythonOperator(
#      task_id='load_data',
#      python_callable=load_data,
#      dag=dag,
# )

# # # Task 2: Check Load Status
# check_load_status_task = PythonOperator(
#      task_id='check_load_status',
#      python_callable=check_load_status,
#      provide_context=True,
#      dag=dag,
# )

# # # Task 3: Print Status
# print_status_task = PythonOperator(
#      task_id='print_status',
#      python_callable=print_status,
#      op_args=[check_load_status_task.output],
#      provide_context=True,
#      dag=dag,
# )


# load_data_task >> check_load_status_task >> print_status_task


# from datetime import datetime
# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# # # Define the DAG
# default_args = {
#      'owner': 'airflow',
#      'start_date': datetime(2023, 9, 1),
#      'retries': 1,
# }

# dag = DAG(
#      'harsha_dag2',
#      default_args=default_args,
#      schedule_interval=None,  
#      catchup=False,
    
# )

# def load_data():
#     try:
#         snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
#         conn = snowflake_hook.get_conn()
#         cursor = conn.cursor()

#         # Truncate the main_table before loading data
#         truncate_query = """
#         TRUNCATE TABLE main_table;
#         """
#         cursor.execute(truncate_query)

#         # Insert data from stage_table into main_table
#         sql_query = """
#         INSERT INTO main_table (Country, Region)
#         SELECT Country, Region
#         FROM stage_table;
#         """
#         cursor.execute(sql_query)

#         cursor.close()
#         conn.close()
#         print("Data loaded successfully")
#         return True
#     except Exception as e:
#         print("Data loading failed -", str(e))
#         return False


# # # Task 1: Load Data from Staging Table to Main
# # def load_data():
# #      try:
# #          snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
# #          conn = snowflake_hook.get_conn()
# #          cursor = conn.cursor()

        
# #          sql_query = """
# #          INSERT INTO main_table (Country, Region)
# #          SELECT Country, Region
# #          FROM stage_table;
# #          """

# #          cursor.execute(sql_query)
# #          cursor.close()
# #          conn.close()
# #          print("Data loaded successfully")
# #          return True
# #      except Exception as e:
# #          print("Data loading failed -", str(e))
# #          return False

# # # Task 2: Check if Load is Successful
# def check_load_status():
#      try:
#          snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
#          conn = snowflake_hook.get_conn()
#          cursor = conn.cursor()

        
#          sql_query = "SELECT * FROM main_table;"
#          cursor.execute(sql_query)
#          row = cursor.fetchall()

        
#          print("Load was successful")
#          return True
             
#      except Exception as e:
#          print("Load failed -", str(e))
#          return False

# # # Task 3: Print Success or Failure Status
# def print_status(load_success):
#      if load_success:
#          print("Success")
#      else:
#          print("Failure")


# # # Task 1: Load Data
# load_data_task = PythonOperator(
#      task_id='load_data',
#      python_callable=load_data,
#      dag=dag,
# )

# # # Task 2: Check Load Status
# check_load_status_task = PythonOperator(
#      task_id='check_load_status',
#      python_callable=check_load_status,
#      provide_context=True,
#      dag=dag,
# )

# # # Task 3: Print Status
# print_status_task = PythonOperator(
#      task_id='print_status',
#      python_callable=print_status,
#      op_args=[check_load_status_task.output],
#      provide_context=True,
#      dag=dag,
# )


# load_data_task >> check_load_status_task >> print_status_task
