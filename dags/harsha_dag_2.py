from airflow import DAG
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# Define your DAG
dag = DAG(
    'load_csv_to_snowflake',
    start_date=days_ago(1),  
    schedule_interval=None,  
    catchup=False  
)

# Define Snowflake connection ID from Airflow's Connection UI
snowflake_conn_id = 'snowflake_conn'

# Define Snowflake target table
snowflake_table = 'bulk_table'

# Define the CSV URL
csv_url = 'https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv'

# Function to load CSV data into Snowflake
def load_csv_to_snowflake():
    try:
        snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

        # Read the CSV file into a DataFrame
        df = pd.read_csv(csv_url)

        # Use SnowflakeHook to insert data into Snowflake table
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()

        # Snowflake COPY INTO command
        copy_into_sql = f'''
        COPY INTO {snowflake_table}
        FROM '{csv_url}'
        FILE_FORMAT = (
            TYPE = 'CSV'
            SKIP_HEADER = 1
        );
        '''

        # Execute the COPY INTO command
        cursor.execute(copy_into_sql)
        cursor.close()
        conn.close()

        print("Data loaded successfully")
        return True
    except Exception as e:
        print("Data loading failed -", str(e))
        return False

# Task to call the load_csv_to_snowflake function
load_csv_task = PythonOperator(
    task_id='load_csv_to_snowflake_task',
    python_callable=load_csv_to_snowflake,
    dag=dag
)

# if __name__ == "__main__":
#     dag.cli()




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
