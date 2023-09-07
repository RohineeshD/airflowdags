from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pydantic import BaseModel, ValidationError, constr
from airflow.models import Variable
from datetime import datetime
import requests
import logging
import pandas as pd
from io import StringIO

# Snowflake connection ID
SNOWFLAKE_CONN_ID = 'snow_sc'

class CsvSettings(BaseModel):
    url: str
    main_table: str = 'sample_csv'
    error_table: str = 'error_log'

class SSNModel(BaseModel):
    ssn: constr(min_length=4, max_length=4)

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
    url = Variable.get("csv_url")
    response = requests.get(url)
    data = response.text
    print(f"Read data from URL. Content: {data}")
    return data

def load_data_to_snowflake(data: str, settings: CsvSettings):
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
    # Data validation using Pydantic
    try:
        CsvSettings(url=settings.url, main_table=settings.main_table, error_table=settings.error_table)
    except ValidationError as e:
        logging.error(f"Invalid settings: {e}")
        return
    
    # Read CSV data into a DataFrame
    df = pd.read_csv(pd.compat.StringIO(data))
    
    # Validate SSN column using Pydantic
    try:
        ssn_model = SSNModel(ssn=df['SSN'].astype(str))
    except ValidationError as e:
        logging.error(f"Invalid SSN values: {e}")
        return
    
    # Split data into valid and invalid based on Pydantic validation
    valid_data = df[ssn_model.index]
    invalid_data = df.drop(ssn_model.index)
    
    # Log the number of rows in the data
    logging.info(f"Number of rows in data: {len(df)}")
    
    # Log the number of valid and invalid rows
    logging.info(f"Number of valid rows: {len(valid_data)}")
    logging.info(f"Number of invalid rows: {len(invalid_data)}")
    
    # Load valid data to main table
    if not valid_data.empty:
        logging.info(f"Loading valid data into Snowflake table: {settings.main_table}...")
        try:
            valid_data.to_sql(settings.main_table, con=snowflake_hook.get_sqlalchemy_engine(), if_exists='append', index=False)
            logging.info(f"Data loaded successfully into {settings.main_table} with {len(valid_data)} rows.")
        except Exception as e:
            logging.error(f"Error loading data into {settings.main_table}: {str(e)}")
    
    # Load invalid data to error_log table
    if not invalid_data.empty:
        logging.info(f"Loading invalid data into Snowflake table: {settings.error_table}...")
        try:
            invalid_data.to_sql(settings.error_table, con=snowflake_hook.get_sqlalchemy_engine(), if_exists='append', index=False)
            logging.info(f"Data loaded successfully into {settings.error_table} with {len(invalid_data)} rows.")
        except Exception as e:
            logging.error(f"Error loading data into {settings.error_table}: {str(e)}")

with dag:
    read_file_task = PythonOperator(
        task_id='read_file_task',
        python_callable=read_file_from_url,
    )

    load_to_snowflake_task = PythonOperator(
        task_id='load_to_snowflake_task',
        python_callable=load_data_to_snowflake,
        op_args=[read_file_task.output, CsvSettings(url=Variable.get("csv_url"))], 
    )

    read_file_task >> load_to_snowflake_task


# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from pydantic import BaseModel, ValidationError
# from airflow.models import Variable
# from datetime import datetime
# import requests
# import logging
# from io import StringIO
# import pandas as pd

# # Snowflake connection ID
# SNOWFLAKE_CONN_ID = 'snow_sc'

# class CsvSettings(BaseModel):
#     url: str
#     main_table: str = 'sample_csv'
#     error_table: str = 'error_log'

# default_args = {
#     'start_date': datetime(2023, 8, 25),
#     'retries': 1,
#     'catchup': True,
# }

# dag_args = {
#     'dag_id': 'charishma_csv_dag',
#     'schedule_interval': None,
#     'default_args': default_args,
#     'catchup': False,
# }
# dag = DAG(**dag_args)

# def read_file_from_url():
#     url = Variable.get("csv_url")
#     response = requests.get(url)
#     data = response.text
#     print(f"Read data from URL. Content: {data}")
#     return data

# def load_data_to_snowflake(data: str, settings: CsvSettings):
#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
#     # Data validation using Pydantic
#     try:
#         CsvSettings(url=settings.url, main_table=settings.main_table, error_table=settings.error_table)
#     except ValidationError as e:
#         logging.error(f"Invalid settings: {e}")
#         return
    
#     # Read CSV data into a DataFrame
#     df = pd.read_csv(pd.compat.StringIO(data))
    
#     # Validate SSN column
#     invalid_data = df[df['SSN'].str.len() != 4]
#     valid_data = df[df['SSN'].str.len() == 4]
    
#     # Log the number of rows in the data
#     logging.info(f"Number of rows in data: {len(df)}")
    
#     # Log the number of valid and invalid rows
#     logging.info(f"Number of valid rows: {len(valid_data)}")
#     logging.info(f"Number of invalid rows: {len(invalid_data)}")
    
#     # Load valid data to main table
#     if not valid_data.empty:
#         logging.info(f"Loading valid data into Snowflake table: {settings.main_table}...")
#         try:
#             valid_data.to_sql(settings.main_table, con=snowflake_hook.get_sqlalchemy_engine(), if_exists='append', index=False)
#             logging.info(f"Data loaded successfully into {settings.main_table} with {len(valid_data)} rows.")
#         except Exception as e:
#             logging.error(f"Error loading data into {settings.main_table}: {str(e)}")
    
#     # Load invalid data to error_log table
#     if not invalid_data.empty:
#         logging.info(f"Loading invalid data into Snowflake table: {settings.error_table}...")
#         try:
#             invalid_data.to_sql(settings.error_table, con=snowflake_hook.get_sqlalchemy_engine(), if_exists='append', index=False)
#             logging.info(f"Data loaded successfully into {settings.error_table} with {len(invalid_data)} rows.")
#         except Exception as e:
#             logging.error(f"Error loading data into {settings.error_table}: {str(e)}")

# with dag:
#     read_file_task = PythonOperator(
#         task_id='read_file_task',
#         python_callable=read_file_from_url,
#     )

#     load_to_snowflake_task = PythonOperator(
#         task_id='load_to_snowflake_task',
#         python_callable=load_data_to_snowflake,
#         op_args=[read_file_task.output, CsvSettings(url=Variable.get("csv_url"))], 
#     )

#     read_file_task >> load_to_snowflake_task




##variable
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from datetime import datetime
# from airflow.models import Variable  #Variables to store and retrieve the URL
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
#     'dag_id': 'charishma_csv_dag',
#     'schedule_interval': None,
#     'default_args': default_args,
#     'catchup': False,
# }
# dag = DAG(**dag_args)

# # Define a Variable for the URL
# url_variable = Variable.get("csv_url")

# def read_file_from_url():
#     #  URL  Variable
#     url = url_variable
#     response = requests.get(url)
#     data = response.text
#     print(f"Read data from URL. Content: {data}")
#     return data

# def load_data_to_snowflake(**kwargs):
#     data = kwargs['task_instance'].xcom_pull(task_ids='read_file_task')
#     df = pd.read_csv(StringIO(data))
    
#     # Ensure 'SSN' column contains string values
#     df['SSN'] = df['SSN'].astype(str)

#     # Define Snowflake tables
#     main_table = 'sample_csv'
#     error_table = 'error_log'
    
#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
#     # Log the number of rows in the DataFrame
#     logging.info(f"Number of rows in DataFrame: {len(df)}")
    
#     # Split data based on SSN criteria
#     valid_data = df[df['SSN'].str.len() == 4]
#     invalid_data = df[df['SSN'].str.len() != 4]
    
#     # Log the number of valid and invalid rows
#     logging.info(f"Number of valid rows: {len(valid_data)}")
#     logging.info(f"Number of invalid rows: {len(invalid_data)}")
    
#     # Load valid data to main table
#     if not valid_data.empty:
#         logging.info("Loading valid data to Snowflake main table...")
#         try:
#             snowflake_hook.insert_rows(main_table, valid_data.values.tolist(), valid_data.columns.tolist())
#             logging.info(f"Data loaded successfully into {main_table} with {len(valid_data)} rows.")
#         except Exception as e:
#             logging.error(f"Error loading data into {main_table}: {str(e)}")
    
#     # Load invalid data to error_log table
#     if not invalid_data.empty:
#         logging.info("Loading invalid data to Snowflake error table...")
#         try:
#             invalid_data['Error_message'] = 'Invalid SSN length should not be more than 4 digits'
#             snowflake_hook.insert_rows(error_table, invalid_data.values.tolist(), invalid_data.columns.tolist())
#             logging.info(f"Data loaded successfully into {error_table} with {len(invalid_data)} rows.")
#         except Exception as e:
#             logging.error(f"Error loading data into {error_table}: {str(e)}")

# with dag:
#     read_file_task = PythonOperator(
#         task_id='read_file_task',
#         python_callable=read_file_from_url,
#     )

#     load_to_snowflake_task = PythonOperator(
#         task_id='load_to_snowflake_task',
#         python_callable=load_data_to_snowflake,
#     )

#     read_file_task >> load_to_snowflake_task


# df
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from datetime import datetime
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
#     'dag_id': 'charishma_csv_dag',
#     'schedule_interval': None,
#     'default_args': default_args,
#     'catchup': False,
# }
# dag = DAG(**dag_args)

# def read_file_from_url():
#     url = "https://raw.githubusercontent.com/jcharishma/my.repo/master/sample_csv.csv"
#     response = requests.get(url)
#     data = response.text
#     print(f"Read data from URL. Content: {data}")
#     return data

# def load_data_to_snowflake(**kwargs):
#     data = kwargs['task_instance'].xcom_pull(task_ids='read_file_task')
#     df = pd.read_csv(StringIO(data))
    
#     # Ensure 'SSN' column contains string values
#     df['SSN'] = df['SSN'].astype(str)

#     # Define Snowflake table names
#     main_table = 'sample_csv'
#     error_table = 'error_log'
    
#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
#     # Log the number of rows in the DataFrame
#     logging.info(f"Number of rows in DataFrame: {len(df)}")
    
#     # Split data based on SSN criteria
#     valid_data = df[df['SSN'].str.len() == 4]
#     invalid_data = df[df['SSN'].str.len() != 4]
    
#     # Log the number of valid and invalid rows
#     logging.info(f"Number of valid rows: {len(valid_data)}")
#     logging.info(f"Number of invalid rows: {len(invalid_data)}")
    
#     # Load valid data to main table
#     if not valid_data.empty:
#         logging.info("Loading valid data to Snowflake main table...")
#         try:
#             snowflake_hook.insert_rows(main_table, valid_data.values.tolist(), valid_data.columns.tolist())
#             logging.info(f"Data loaded successfully into {main_table} with {len(valid_data)} rows.")
#         except Exception as e:
#             logging.error(f"Error loading data into {main_table}: {str(e)}")
    
#     # Load invalid data to error_log table
#     if not invalid_data.empty:
#         logging.info("Loading invalid data to Snowflake error table...")
#         try:
#             invalid_data['Error_message'] = 'Invalid SSN length should not be more than 4 digits'
#             snowflake_hook.insert_rows(error_table, invalid_data.values.tolist(), invalid_data.columns.tolist())
#             logging.info(f"Data loaded successfully into {error_table} with {len(invalid_data)} rows.")
#         except Exception as e:
#             logging.error(f"Error loading data into {error_table}: {str(e)}")

# with dag:
#     read_file_task = PythonOperator(
#         task_id='read_file_task',
#         python_callable=read_file_from_url,
#     )

#     load_to_snowflake_task = PythonOperator(
#         task_id='load_to_snowflake_task',
#         python_callable=load_data_to_snowflake,
#     )

#     read_file_task >> load_to_snowflake_task
#################

# def load_data_to_snowflake(**kwargs):
#     data = kwargs['task_instance'].xcom_pull(task_ids='read_file_task')
#     df = pd.read_csv(StringIO(data))
    
#     # Ensure 'SSN' column contains string values
#     # df['SSN'] = df['SSN'].astype(str)
#     df['SSN'] = df['SSN'].astype(str)

#     # Define Snowflake table names
#     main_table = 'sample_csv'
#     error_table = 'error_log'
    
#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
#     # Log the number of rows in the DataFrame
#     logging.info(f"Number of rows in DataFrame: {len(df)}")
    
#     # Split data based on SSN criteria
#     valid_data = df[df['SSN'].str.len() == 4]
#     invalid_data = df[df['SSN'].str.len() != 4]
    
#     # Log the number of valid and invalid rows
#     logging.info(f"Number of valid rows: {len(valid_data)}")
#     logging.info(f"Number of invalid rows: {len(invalid_data)}")
    
#     # Load valid data to main table
#     if not valid_data.empty:
#         logging.info("Loading valid data to Snowflake main table...")
#         snowflake_hook.insert_rows(main_table, valid_data.values.tolist(), valid_data.columns.tolist())
    
#     # Load invalid data to error_log table
#     if not invalid_data.empty:
#         logging.info("Loading invalid data to Snowflake error table...")
#         invalid_data['Error_message'] = 'Invalid SSN length should not be more than 4 digits'
#         snowflake_hook.insert_rows(error_table, invalid_data.values.tolist(), invalid_data.columns.tolist())






# def load_data_to_snowflake(**kwargs):
#     data = kwargs['task_instance'].xcom_pull(task_ids='read_file_task')
#     df = pd.read_csv(StringIO(data))
    
#     # Ensure 'SSN' column contains string values
#     df['SSN'] = df['SSN'].astype(str)

#     # Define Snowflake table names
#     main_table = 'sample_csv'
#     error_table = 'error_log'
    
#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
#     # Log the number of rows in the DataFrame
#     logging.info(f"Number of rows in DataFrame: {len(df)}")
    
#     # Filter rows with SSN length not equal to 4
#     invalid_data = df[df['SSN'].str.len() != 4]
    
#     # Log the number of invalid rows
#     logging.info(f"Number of invalid rows: {len(invalid_data)}")
    
#     # Load invalid data to error_log table
#     if not invalid_data.empty:
#         logging.info("Loading invalid data to Snowflake error table...")
#         invalid_data['Name'] = invalid_data['name']
#         invalid_data['Invalid_ssn'] = invalid_data['SSN']
#         invalid_data['Error_message'] = 'Invalid SSN length (should be 4)'
#         snowflake_hook.insert_rows(error_table, invalid_data[['Name', 'Invalid_ssn', 'Error_message']].values.tolist(), ['Name', 'Invalid_ssn', 'Error_message'])







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
