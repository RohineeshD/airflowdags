from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.exceptions import AirflowException
import pandas as pd
import requests

# Define your DAG with appropriate configurations
dag = DAG(
    'csv_upload_to_snowflake',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
)

def decide_branch(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='load_data_to_snowflake')
    return 'success_task' if result else 'failure_task'

# Define the Snowflake data load task (task1)
def load_data_to_snowflake(**kwargs):
    try:
        url = 'https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv'
        response = requests.get(url)
        if response.status_code == 200:
            csv_data = response.text
            # Parse CSV data into a DataFrame
            df = pd.read_csv(pd.compat.StringIO(csv_data))
            
            # Assuming you have a Snowflake connection already set up in Airflow
            # You should replace 'YOUR_SNOWFLAKE_TABLE' and define the SnowflakeOperator accordingly
            snowflake_operator = SnowflakeOperator(
                task_id='load_data_to_snowflake',
                sql=(
                    "INSERT INTO CSV_ATBLE "
                    "(NAME, EMAIL, SSN) "
                    "VALUES (%s, %s, %s)"
                ),  # Replace with your SQL INSERT statement
                snowflake_conn_id='snow_sc',
                autocommit=True,
                parameters=[tuple(row) for row in df.values],  # Convert DataFrame rows to tuples
                dag=dag,
            )
            snowflake_operator.execute(context=kwargs)
            print("Data loaded successfully.")
            return True
        else:
            print("Failed to download CSV data from the URL.")
            return False
    except Exception as e:
        # Log the exception
        print(f"Error loading data to Snowflake: {str(e)}")
        raise AirflowException("Error loading data to Snowflake")

# Define the BranchPythonOperator as task2
task2 = BranchPythonOperator(
    task_id='task2',
    python_callable=decide_branch,
    provide_context=True,
    dag=dag,
)

def success_task(**kwargs):
    print("CSV data loaded successfully into Snowflake!")
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='load_data_to_snowflake')
    print(f"XCom result from load_data_to_snowflake task: {result}")

def failure_task(**kwargs):
    print("Failed to load CSV data into Snowflake!")
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='load_data_to_snowflake')
    print(f"XCom result from load_data_to_snowflake task: {result}")

# Define the task to print success or failure
success_print_task = PythonOperator(
    task_id='success_print',
    python_callable=success_task,
    provide_context=True,
    dag=dag,
)

failure_print_task = PythonOperator(
    task_id='failure_print',
    python_callable=failure_task,
    provide_context=True,
    dag=dag,
)

# Set up task dependencies
load_data_to_snowflake_task = PythonOperator(
    task_id='load_data_to_snowflake',
    python_callable=load_data_to_snowflake,
    provide_context=True,
    dag=dag,
)

load_data_to_snowflake_task >> task2 >> [success_print_task, failure_print_task]








# #validate ssn pydantic
# import pandas as pd
# from pydantic import BaseModel, ValidationError, validator
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# import requests
# from io import StringIO
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# # Define your CSV URL
# CSV_URL = 'https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv'

# default_args = {
#     'owner': 'airflow',
#     'start_date': days_ago(1),
# }

# dag = DAG(
#     'csv_dag',
#     default_args=default_args,
#     schedule_interval=None, 
#     catchup=False,
# )

# # Define Pydantic model for validation
# class CsvRow(BaseModel):
#     NAME: str
#     EMAIL: str
#     SSN: int
    
#     @validator('SSN')
#     def validate_id(cls, SSN):
#         if not (1000 <= SSN <= 9999):
#             raise ValueError('Invalid SSN length; it should be 4 digits')
#         return SSN
# # Task 1: Read and display the CSV file from the URL
# def read_and_display_csv(csv_url):
#     df = pd.read_csv(csv_url)
#     print(df)

# read_file_task = PythonOperator(
#     task_id='read_file',
#     python_callable=read_and_display_csv,
#     op_args=[CSV_URL], 
#     dag=dag,
# )

# # Task 2: Load data to tables and log errors
# def fetch_and_validate_csv():
#     valid_rows=[]
    
#     # Replace with your Snowflake schema and table name
#     try:
#         # Fetch data from CSV URL
#         response = requests.get(CSV_URL)
#         response.raise_for_status()

#         # Read CSV data into a DataFrame
#         df = pd.read_csv(StringIO(response.text))
#         # Iterate through rows and validate each one
#         errors = []
#         for index, row in df.iterrows():
#             try:
#                 validated_row=CsvRow(**row.to_dict())
#                 print(validated_row)
#                 valid_rows.append((validated_row.NAME, validated_row.EMAIL, validated_row.SSN))
#             # except Exception as e:
#             #     status = f"CHECK SSN IT SHOULD HAVE 4 DIGIT NUMBER: {str(e)}"
#             except ValidationError as e:
#                 # Record validation errors and add them to the "error" column
#                 error_message = f"Invalid SSN length; it should be 4 digits: {str(e)}"
#                 errors.append(error_message)
#                 df.at[index, 'error'] = error_message
#         # Add a new column to the DataFrame if not already present
#         if 'error' not in df.columns:
#             df['error'] = "--"
        
#         print(df)
#         # Check for NaN values in the entire DataFrame
#         has_nan = df.isnull().values.any()
        
#         # If there are NaN values, replace them with 0
#         if has_nan:
#             df.fillna(0, inplace=True)
#         snowflake_hook = SnowflakeHook(snowflake_conn_id='snow_sc')
#         schema = 'SC1'
#         table_name = 'SAMPLE_CSV_ERROR'
#         connection = snowflake_hook.get_conn()
#         snowflake_hook.insert_rows(table_name, df.values.tolist())
#         connection.close()
#         print(f"CSV at {CSV_URL} has been validated successfully.")
    
#     except Exception as e:
#         print(f"Error: {str(e)}")
       
        
#     snowflake_hook = SnowflakeHook(snowflake_conn_id='snow_sc')
#     schema = 'SC1'
#     table_name = 'SAMPLE_CSV'
#     connection = snowflake_hook.get_conn()
#     snowflake_hook.insert_rows(table_name, valid_rows)
#     connection.close()


# validate_load_task = PythonOperator(
#     task_id='validate_load_task',
#     python_callable=fetch_and_validate_csv,
#     provide_context=True,
#     dag=dag,
# )

# # Set up task dependencies
# read_file_task >> validate_load_task


# def fetch_and_validate_csv():
#     valid_rows = []
#     errors = []
    
#     try:
#         # Fetch data from CSV URL
#         response = requests.get(CSV_URL)
#         response.raise_for_status()

#         # Read CSV data into a DataFrame
#         df = pd.read_csv(StringIO(response.text))
        
#         # Iterate through rows and validate each one
#         for index, row in df.iterrows():
#             try:
#                 validated_row = CsvRow(**row.to_dict())
#                 valid_rows.append((validated_row.NAME, validated_row.EMAIL, validated_row.SSN))
#             except ValidationError as e:
#                 # Record validation errors and add them to the "error" column
#                 error_message = f"SSN SHOULD HAVE 4 DIGIT NUMBER: {str(e)}"
#                 errors.append(error_message)
#                 df.at[index, 'error'] = error_message
        
#         if 'error' not in df.columns:
#             df['error'] = "--"
        
#         print(df)
        
#         # Check for NaN values in the entire DataFrame
#         has_nan = df.isnull().values.any()
        
#         # If there are NaN values, replace them with 0
#         if has_nan:
#             df.fillna(0, inplace=True)
        
#         snowflake_hook = SnowflakeHook(snowflake_conn_id='snow_sc')
#         schema = 'SC1'
        
#         # Insert errors into the ERROR_LOG table
#         if errors:
#             error_df = pd.DataFrame({'error_message': errors})
#             table_name = 'ERROR_LOG'
#             connection = snowflake_hook.get_conn()
#             snowflake_hook.insert_rows(table_name, error_df.values.tolist())
#             connection.close()
        
#         # Insert valid data into the SAMPLE_CSV table
#         table_name = 'SAMPLE_CSV'
#         connection = snowflake_hook.get_conn()
#         snowflake_hook.insert_rows(table_name, valid_rows)
#         connection.close()
        
#         print(f"CSV at {CSV_URL} has been validated successfully.")
    
#     except Exception as e:
#         print(f"Error: {str(e)}")




# import pandas as pd
# from pydantic import BaseModel, ValidationError, validator
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# import requests
# from io import StringIO
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# # Define your CSV URL
# CSV_URL = 'https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv'

# default_args = {
#     'owner': 'airflow',
#     'start_date': days_ago(1),
# }

# dag = DAG(
#     'csv_dag',
#     default_args=default_args,
#     schedule_interval=None, 
#     catchup=False,
# )

# # Define Pydantic model for validation
# class CsvRow(BaseModel):
#     NAME: str
#     EMAIL: str
#     SSN: int
#     @validator('SSN')
#     def validate_id(cls, SSN):
#         if not (1000 <= SSN <= 9999):
#             raise ValueError('Invalid SSN length; it should be 4 digits')
#         return SSN


# # Task 1: Read and display the CSV file from the URL
# def read_and_display_csv():
#     csv_url = 'https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv'
#     df = pd.read_csv(csv_url)
#     print(df)

# read_file_task = PythonOperator(
#     task_id='read_file',
#     python_callable=read_and_display_csv,
#     dag=dag,
# )
# # Task 2: load data to tables
# def fetch_and_validate_csv():
#     valid_rows=[]
    
    
#     try:
#         # Fetch data from CSV URL
#         response = requests.get(CSV_URL)
#         response.raise_for_status()

#         # Read CSV data into a DataFrame
#         df = pd.read_csv(StringIO(response.text))
#         # Iterate through rows and validate each one
#         errors = []
#         for index, row in df.iterrows():
#             try:
#                 validated_row=CsvRow(**row.to_dict())
#                 print(validated_row)
#                 valid_rows.append((validated_row.NAME, validated_row.EMAIL, validated_row.SSN))
        
#             except ValidationError as e:
#                 # Record validation errors and add them to the "error" column
#                 error_message = f"SSN  SHOULD HAVE 4 DIGIT NUMBER: {str(e)}"
#                 errors.append(error_message)
#                 df.at[index, 'error'] = error_message
       
#         if 'error' not in df.columns:
#             df['error'] = "--"
        
#         print(df)
#         # Check for NaN values in the entire DataFrame
#         has_nan = df.isnull().values.any()
        
#         # If there are NaN values, replace them with 0
#         if has_nan:
#             df.fillna(0, inplace=True)
#         snowflake_hook = SnowflakeHook(snowflake_conn_id='snow_sc')
#         schema = 'SC1'
#         table_name = 'ERROR_LOG'
#         connection = snowflake_hook.get_conn()
#         snowflake_hook.insert_rows(table_name, df.values.tolist())
#         connection.close()
#         print(f"CSV at {CSV_URL} has been validated successfully.")
    
#     except Exception as e:
#         print(f"Error: {str(e)}")
       
        
#     snowflake_hook = SnowflakeHook(snowflake_conn_id='snow_sc')
#     # Replace with your Snowflake schema and table name
#     schema = 'SC1'
#     table_name = 'SAMPLE_CSV'
#     connection = snowflake_hook.get_conn()
#     snowflake_hook.insert_rows(table_name, valid_rows)
#     connection.close()
    

# validate_load_task = PythonOperator(
#     task_id='validate_load_task',
#     python_callable=fetch_and_validate_csv,
#     provide_context=True,
#     dag=dag,
# )

# # Set up task dependencies
# read_file_task >> validate_load_task



# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from pydantic import BaseModel, ValidationError, validator
# from datetime import datetime
# import requests
# import json
# import snowflake.connector
# import pandas as pd
# import os
# import math
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# # Define the DAG
# dag = DAG(
#     'csv_dag',
#     start_date=datetime(2023, 1, 1),
#     schedule_interval=None,
#     catchup=False,
# )

# # Task 1: Read and display the CSV file from the URL
# def read_and_display_csv():
#     csv_url = 'https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv'
#     df = pd.read_csv(csv_url)
#     print(df)

# read_file_task = PythonOperator(
#     task_id='read_file',
#     python_callable=read_and_display_csv,
#     dag=dag,
# )

# class CSVRecord(BaseModel):
#     NAME: str
#     EMAIL: str
#     SSN: str

#     @validator('SSN')
#     def validate_ssn(cls, ssn):
#         # if ssn is None:
#         #     raise ValueError("SSN is miss")
        
#         if len(ssn) != 4:
#             raise ValueError("Invalid SSN length; it should be 4 digits")

#         return ssn

# # validate and load data from URL
# def validate_and_load_data(**kwargs):
#     csv_url = 'https://raw.githubusercontent.com/jcharishma/my.repo/master/sample_csv.csv'

#     response = requests.get(csv_url)
#     if response.status_code == 200:
#         csv_content = response.text
#         csv_lines = csv_content.split('\n')
#         header = None

#         # Snowflake connection
#         snowflake_hook = SnowflakeHook(snowflake_conn_id="snow_sc")
#         conn = snowflake_hook.get_conn()
#         cursor = conn.cursor()

#         for line in csv_lines:
#             line = line.strip()
#             if not line:
#                 continue
#             if not header:
#                 header = line.split(',')
#                 continue

#             row = line.split(',')
#             if len(row) != len(header):
#                 continue

#             ssn = row[2].strip()
        
#             if len(ssn) != 4:
#                 print(len(ssn), type(ssn))
#                 if len(ssn)==0:
#                     ssn ="0"
#                     error_msg = "SSN IS NULL"
#                 else:
#                     error_msg = "Invalid SSN length; it should be 4 digits"
#                 insert_error_sql = f"INSERT INTO ERROR_LOG (NAME, EMAIL, SSN, ERROR_MESSAGE) VALUES ('{row[0]}', '{row[1]}', '{ssn}', '{error_msg}')"
#                 cursor.execute(insert_error_sql)
#                 conn.commit()
               
#             else:
#                 try:
#                     record = CSVRecord(NAME=row[0], EMAIL=row[1], SSN=ssn)
#                     insert_sql = f"INSERT INTO SAMPLE_CSV (NAME, EMAIL, SSN) VALUES ('{record.NAME}', '{record.EMAIL}', '{record.SSN}')"
#                     cursor.execute(insert_sql)
#                     conn.commit()
#                 except Exception as e:
#                     print(f"Error: {str(e)}")

#         # Close connection
#         cursor.close()
#         conn.close()

# # Define the task to validate and load data
# validate_load_task = PythonOperator(
#     task_id='validate_and_load_data',
#     python_callable=validate_and_load_data,
#     provide_context=True,
#     dag=dag,
# )

# # Set up task dependencies
# read_file_task >> validate_load_task








# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from pydantic import BaseModel, ValidationError, validator
# from datetime import datetime
# import requests
# import json
# import snowflake.connector
# import pandas as pd
# import os
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


# # Define the DAG
# dag = DAG(
#     'csv_dag',
#     start_date=datetime(2023, 1, 1),
#     schedule_interval=None,
#     catchup=False,
# )

# # Task 1: Read and display the CSV file from the URL
# def read_and_display_csv():
#     csv_url = 'https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv'
#     df = pd.read_csv(csv_url)
#     print(df)

# read_file_task = PythonOperator(
#     task_id='read_file',
#     python_callable=read_and_display_csv,
#     dag=dag,
# )

# # Pydantic model for CSV records
# class CSVRecord(BaseModel):
#     NAME: str
#     EMAIL: str
#     SSN: str

#     @validator('SSN')
#     def validate_ssn(cls, ssn):
#         if len(ssn) != 4:
#             return ssn
#         elif ssn is None:
#             raise ValueError("SSN is none")
#         else:
#             raise ValueError("Invalid SSN length should not be more than 4 digits")

#         # if ssn is None:
#         #     raise ValueError("SSN is None")

#         # if len(ssn) != 4:
#         #     raise ValueError("Invalid SSN length; it should be 4 digits")
        
#         # return ssn
#         #     else:
#         #             raise ValueError("SSN is missing")
#         # except Exception as e:
#         #         print(f"Error: {str(e)}")
       
        
# # validate and load data from URL
# def validate_and_load_data(**kwargs):
#     csv_url = 'https://raw.githubusercontent.com/jcharishma/my.repo/master/sample_csv.csv'

#     response = requests.get(csv_url)
#     if response.status_code == 200:
#         csv_content = response.text
#         csv_lines = csv_content.split('\n')
#         header = None

#         #  Snowflake connection 
#         snowflake_hook = SnowflakeHook(snowflake_conn_id="snow_sc")
#         conn = snowflake_hook.get_conn()
#         cursor = conn.cursor()

#         for line in csv_lines:
#             line = line.strip()
#             if not line:
#                 continue
#             if not header:
#                 header = line.split(',')
#                 continue

#             row = line.split(',')
#             if len(row) != len(header):
#                 continue

#             try:
#                 record = CSVRecord(NAME=row[0], EMAIL=row[1], SSN=row[2])
#                 insert_sql = f"INSERT INTO SAMPLE_CSV (NAME, EMAIL, SSN) VALUES ('{record.NAME}', '{record.EMAIL}', '{record.SSN}')"
#                 cursor.execute(insert_sql)
#                 conn.commit()
#             except ValidationError as e:
#                 for error in e.errors():
#                     field_name = error.get('loc')[-1]
#                     error_msg = error.get('msg')
#                     print(f"Validation Error for {field_name}: {error_msg}")
#                     # For invalid SSN, insert into ERROR_LOG table
#                     insert_error_sql = f"INSERT INTO ERROR_LOG (NAME, EMAIL, SSN, ERROR_MESSAGE) VALUES ('{row[0]}', '{row[1]}', '{row[2]}', '{error_msg}')"
#                     cursor.execute(insert_error_sql)
#                     conn.commit()
#             except Exception as e:
#                 print(f"Error: {str(e)}")

#         # Close connection
#         cursor.close()
#         conn.close()


# validate_load_task = PythonOperator(
#     task_id='validate_and_load_data',
#     python_callable=validate_and_load_data,
#     provide_context=True,
#     dag=dag,
# )


# read_file_task >> validate_load_task



# data not loading
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from pydantic import BaseModel, ValidationError, validator
# from datetime import datetime
# import requests
# import json
# import snowflake.connector
# import pandas as pd
# import os

# # Define the DAG
# dag = DAG(
#     'csv_dag',
#     start_date=datetime(2023, 1, 1),
#     schedule_interval=None,
#     catchup=False,
# )

# # Task 1: Read and display the CSV file from the URL
# def read_and_display_csv():
#     csv_url = 'https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv'
#     df = pd.read_csv(csv_url)
#     print(df)

# read_file_task = PythonOperator(
#     task_id='read_file',
#     python_callable=read_and_display_csv,
#     dag=dag,
# )

# # Pydantic model for CSV records
# class CSVRecord(BaseModel):
#     NAME: str
#     EMAIL: str
#     SSN: str

#     @validator('SSN')
#     def validate_ssn_length(cls, ssn):
#         if len(ssn) != 4:
#             raise ValueError("SSN length should be 4 digits")
#         return ssn

# # Function to validate and load data from a CSV URL
# def validate_and_load_data(**kwargs):
#     csv_url = 'https://raw.githubusercontent.com/jcharishma/my.repo/master/sample_csv.csv'

#     response = requests.get(csv_url)
#     if response.status_code == 200:
#         csv_content = response.text
#         csv_lines = csv_content.split('\n')
#         header = None

#         # Load Snowflake credentials from creds.json
#         creds_file_path = 'C:/Users/chari/OneDrive/Desktop/creds.json'
#         if os.path.exists(creds_file_path):
#             with open(creds_file_path, 'r') as creds_file:
#                 snowflake_credentials = json.load(creds_file)

#             # Establish Snowflake connection using the loaded credentials
#             conn = snowflake.connector.connect(**snowflake_credentials)

#             cursor = conn.cursor()

#             for line in csv_lines:
#                 line = line.strip()
#                 if not line:
#                     continue
#                 if not header:
#                     header = line.split(',')
#                     continue

#                 row = line.split(',')
#                 if len(row) != len(header):
#                     continue

#                 try:
#                     record = CSVRecord(NAME=row[0], EMAIL=row[1], SSN=row[2])
#                     insert_sql = f"INSERT INTO SAMPLE_CSV (NAME, EMAIL, SSN) VALUES ('{record.NAME}', '{record.EMAIL}', '{record.SSN}')"
#                     cursor.execute(insert_sql)
#                     conn.commit()
#                 except ValidationError as e:
#                     for error in e.errors():
#                         field_name = error.get('loc')[-1]
#                         error_msg = error.get('msg')
#                         print(f"Validation Error for {field_name}: {error_msg}")
#                         # For invalid SSN, insert into ERROR_LOG table
#                         insert_error_sql = f"INSERT INTO ERROR_LOG (NAME, EMAIL, SSN, ERROR_MESSAGE) VALUES ('{record.NAME}', '{record.EMAIL}', '{record.SSN}', '{error_msg}')"
#                         cursor.execute(insert_error_sql)
#                         conn.commit()
#                 except Exception as e:
#                     print(f"Error: {str(e)}")

#             # Close Snowflake connection
#             cursor.close()
#             conn.close()

# validate_load_task = PythonOperator(
#     task_id='validate_and_load_data',
#     python_callable=validate_and_load_data,
#     provide_context=True,  # 
#     dag=dag,
# )

# # Set task dependencies
# read_file_task >> validate_load_task




# using connectionID
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from pydantic import BaseModel, ValidationError, validator
# from datetime import datetime
# import requests
# import snowflake.connector
# import pandas as pd

# # Snowflake connection ID
# SNOWFLAKE_CONN_ID = 'snow_sc'

# # Define the DAG
# dag = DAG(
#     'csv_dag',
#     start_date=datetime(2023, 1, 1),
#     schedule_interval=None,  
#     catchup=False,
# )

# # Task 1: Read and display the CSV file from the URL
# def read_and_display_csv():
#     csv_url = 'https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv'
#     df = pd.read_csv(csv_url)
#     print(df)

# read_file_task = PythonOperator(
#     task_id='read_file',
#     python_callable=read_and_display_csv,
#     dag=dag,
# )

# # Pydantic model for CSV records
# class CSVRecord(BaseModel):
#     NAME: str
#     EMAIL: str
#     SSN: str

#     @validator('SSN')
#     def validate_ssn_length(cls, ssn):
#         if len(ssn) != 4:
#             raise ValueError("SSN length should be 4 digits")
#         return ssn

# def validate_and_load_data():
#     csv_url = 'https://raw.githubusercontent.com/jcharishma/my.repo/master/sample_csv.csv'

#     response = requests.get(csv_url)
#     if response.status_code == 200:
#         csv_content = response.text
#         csv_lines = csv_content.split('\n')
#         header = None

#         # Establish Snowflake connection
#         conn = snowflake.connector.connect(
#             connection_name=SNOWFLAKE_CONN_ID
#         )

#         cursor = conn.cursor()

#         for line in csv_lines:
#             line = line.strip()
#             if not line:
#                 continue
#             if not header:
#                 header = line.split()
#                 continue

#             row = line.split()
#             if len(row) != len(header):
#                 continue 

#             try:
#                 record = CSVRecord(NAME=row[0], EMAIL=row[1], SSN=row[2])
#                 if len(record.SSN) == 4:
#                     # Insert valid records into the SAMPLE_CSV table
#                     insert_sql = f"INSERT INTO SAMPLE_CSV (NAME, EMAIL, SSN) VALUES ('{record.NAME}', '{record.EMAIL}', '{record.SSN}')"
#                     cursor.execute(insert_sql)
#                     conn.commit()
#                 else:
#                     # inserting  record into the ERROR_LOG table
#                     error_message = 'Invalid SSN length should be 4 digits'
#                     insert_error_sql = f"INSERT INTO ERROR_LOG (NAME, EMAIL, SSN, ERROR_MESSAGE) VALUES ('{record.NAME}', '{record.EMAIL}', '{record.SSN}', '{error_message}')"
#                     cursor.execute(insert_error_sql)
#                     conn.commit()
#             except ValidationError as e:
#                 for error in e.errors():
#                     field_name = error.get('loc')[-1]
#                     error_msg = error.get('msg')
#                     # error message
#                     print(f"Validation Error for {field_name}: {error_msg}")
#             except Exception as e:
#                 print(f"Error: {str(e)}")

#         # Close Snowflake connection
#         cursor.close()
#         conn.close()


# validate_load_task = PythonOperator(
#     task_id='validate_and_load_data',
#     python_callable=validate_and_load_data,
#     dag=dag,
# )

# # Set task dependencies
# read_file_task >> validate_load_task









# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# from pydantic import BaseModel, ValidationError, constr
# import pandas as pd
# from airflow.hooks.base_hook import BaseHook
# import snowflake.connector

# # Define the default_args for the DAG
# default_args = {
#     'owner': 'airflow',
#     'start_date': days_ago(1),
#     'retries': 1,
# }

# # Define the Snowflake connection ID
# snowflake_conn_id = 'snow_sc'

# # Define the Pydantic model for validation
# class CSVRecord(BaseModel):
#     SSN: str  # We change this to str to load data as-is

# # Define the DAG
# with DAG(
#     'csv_dag',
#     default_args=default_args,
#     schedule_interval=None,  # You can adjust the schedule as needed
#     catchup=False,
# ) as dag:

#     # Task 1: Read and display the CSV file from the URL
#     def read_and_display_csv():
#         csv_url = "https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv"
#         df = pd.read_csv(csv_url)
#         print(df)
    
#     read_csv_task = PythonOperator(
#         task_id='read_csv',
#         python_callable=read_and_display_csv,
#     )

#     # Task 2: Load data into Snowflake tables and perform validation
#     def load_data_into_snowflake():
#         try:
#             # Initialize Snowflake hook
#             snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
#             connection = snowflake_hook.get_conn()
#             cursor = connection.cursor()

#             # Define Snowflake table names
#             sample_csv_table = "SAMPLE_CSV"
#             error_log_table = "ERROR_LOG"

#             # Read the CSV file into a DataFrame
#             csv_url = "https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv"
#             df = pd.read_csv(csv_url)

#             for index, row in df.iterrows():
#                 try:
#                     # Since we changed the Pydantic model to treat SSN as a string, we can insert it directly
#                     # without any conversion
#                     cursor.execute(f"INSERT INTO {sample_csv_table} VALUES (?)", (row['SSN'],))

#                 except ValidationError as e:
#                     # If validation fails, insert the record into ERROR_LOG table with error message
#                     cursor.execute(f"INSERT INTO {error_log_table} VALUES (?, ?)", (row.to_dict(), str(e)))

#             # Commit the changes to the Snowflake database
#             connection.commit()

#         except Exception as e:
#             print(f"Error: {e}")
#         finally:
#             # Close the cursor and connection
#             cursor.close()
#             connection.close()

#     load_data_task = PythonOperator(
#         task_id='load_data_into_snowflake',
#         python_callable=load_data_into_snowflake,
#     )

#     # Set task dependencies
#     read_csv_task >> load_data_task

# if __name__ == "__main__":
#     dag.cli()






# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from datetime import datetime
# import requests
# from pydantic import BaseModel, ValidationError, constr

# # Create a function to establish the Snowflake connection using SnowflakeHook
# def create_snowflake_connection():
#     hook = SnowflakeHook(snowflake_conn_id="snow_sc")
#     conn = hook.get_conn()
#     return conn

# # Define the Pydantic model for CSV data
# class CSVRecord(BaseModel):
#     NAME: constr(strip_whitespace=True, min_length=1)
#     EMAIL: constr(strip_whitespace=True, min_length=1, regex=r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$')
#     SSN: constr(
#         strip_whitespace=True,
#         regex=r'^\d{4}$',  # Check for exactly 4 digits
#     )

# # Function to bulk insert valid records into SAMPLE_CSV
# def insert_valid_records(records, snowflake_conn):
#     if records:
#         cursor = snowflake_conn.cursor()
#         try:
#             cursor.executemany(
#                 f"""
#                 INSERT INTO SAMPLE_CSV (NAME, EMAIL, SSN)
#                 VALUES (?, ?, ?)
#                 """,
#                 records,
#             )
#             snowflake_conn.commit()
#         finally:
#             cursor.close()

# # Function to bulk insert error records into ERROR_LOG
# def insert_error_records(records, snowflake_conn):
#     if records:
#         cursor = snowflake_conn.cursor()
#         try:
#             cursor.executemany(
#                 f"""
#                 INSERT INTO ERROR_LOG (NAME, EMAIL, SSN, ERROR_MESSAGE)
#                 VALUES (?, ?, ?, ?)
#                 """,
#                 records,
#             )
#             snowflake_conn.commit()
#         finally:
#             cursor.close()

# # Task to read file from the provided URL and display data
# def read_file_and_display_data():
#     # Input CSV file URL
#     csv_url = 'https://raw.githubusercontent.com/jcharishma/my.repo/master/sample_csv.csv'

#     # Fetch CSV data from the URL
#     response = requests.get(csv_url)
#     if response.status_code == 200:
#         csv_content = response.text
#         print("CSV Data:")
#         print(csv_content)
#         return csv_content
#     else:
#         raise Exception(f"Failed to fetch CSV: Status Code {response.status_code}")

# # Task to validate and load data using Pydantic
# def validate_and_load_data():
#     snowflake_conn = create_snowflake_connection()

#     # Input CSV file URL
#     csv_url = 'https://raw.githubusercontent.com/jcharishma/my.repo/master/sample_csv.csv'

#     # Fetch CSV data from the URL
#     response = requests.get(csv_url)
#     if response.status_code == 200:
#         csv_content = response.text
#         csv_lines = csv_content.split('\n')
#         header = None
#         valid_records = []
#         error_records = []

#         for line in csv_lines:
#             line = line.strip()
#             if not line:
#                 continue
#             if not header:
#                 header = line.split('\t')  # Split by tab
#                 continue
#             row = line.split('\t')
#             if len(row) != len(header):
#                 # Invalid format, add to error_records
#                 error_records.append((row[0], row[1], row[2], 'Invalid CSV format'))
#                 continue

#             try:
#                 record = CSVRecord(NAME=row[0], EMAIL=row[1], SSN=row[2])
#                 record_dict = record.dict()
#                 valid_records.append((record_dict['NAME'], record_dict['EMAIL'], record_dict['SSN']))
#             except ValidationError as e:
#                 for error in e.errors():
#                     field_name = error['loc'][-1]
#                     error_msg = error['msg']
#                     # Add to error_records
#                     error_records.append((row[0], row[1], row[2], error_msg))
#             except Exception as e:
#                 # Handle other exceptions as needed
#                 print(f"Error: {str(e)}")

#         # Bulk insert valid and error records
#         insert_valid_records(valid_records, snowflake_conn)
#         insert_error_records(error_records, snowflake_conn)

#     # Close Snowflake connection
#     snowflake_conn.close()

# # Airflow default arguments
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 9, 7),
#     'retries': 1,
#     'catchup': True,
# }

# # Create the DAG
# dag = DAG(
#     'csv_dag',
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
# )

# # Task to read file from the provided URL and display data
# read_file_task = PythonOperator(
#     task_id='read_file_and_display_data',
#     python_callable=read_file_and_display_data,
#     dag=dag,
# )

# # Task to validate and load data using Pydantic
# validate_task = PythonOperator(
#     task_id='validate_and_load_data',
#     python_callable=validate_and_load_data,
#     dag=dag,
# )

# # Set task dependencies
# read_file_task >> validate_task






# ...

# # Task to validate and load data using Pydantic
# def validate_and_load_data():
#     snowflake_conn = create_snowflake_connection()

#     # Input CSV file URL
#     csv_url = 'https://raw.githubusercontent.com/jcharishma/my.repo/master/sample_csv.csv'

#     # Fetch CSV data from the URL
#     response = requests.get(csv_url)
#     if response.status_code == 200:
#         csv_content = response.text
#         csv_lines = csv_content.split('\n')
#         header = None
#         for line in csv_lines:
#             line = line.strip()
#             if not line:
#                 continue
#             if not header:
#                 header = re.split(r'\s+', line)  # Split by varying spaces
#                 continue
#             row = re.split(r'\s+', line)
#             if len(row) != len(header):
#                 # Invalid format, insert into ERROR_LOG table
#                 insert_error_task = SnowflakeOperator(
#                     task_id='insert_into_error_log',
#                     sql=f"""
#                         INSERT INTO ERROR_LOG (NAME, EMAIL, SSN, ERROR_MESSAGE)
#                         VALUES ('{row[0]}', '{row[1]}', '{row[2]}', 'Invalid CSV format')
#                     """,
#                     snowflake_conn_id="snow_sc",  # Connection ID defined in Airflow
#                     dag=dag,
#                 )
#                 insert_error_task.execute(snowflake_conn)
#                 continue

#             try:
#                 record = CSVRecord(NAME=row[0], EMAIL=row[1], SSN=row[2])
#                 if len(record.SSN) == 4:
#                     # Insert into SAMPLE_CSV table
#                     insert_task = SnowflakeOperator(
#                         task_id='insert_into_sample_csv',
#                         sql=f"""
#                             INSERT INTO SAMPLE_CSV (NAME, EMAIL, SSN)
#                             VALUES ('{record.NAME}', '{record.EMAIL}', '{record.SSN}')
#                         """,
#                         snowflake_conn_id="snow_sc",  # Connection ID defined in Airflow
#                         dag=dag,
#                     )
#                     insert_task.execute(snowflake_conn)
#                 else:
#                     # Insert into ERROR_LOG table
#                     insert_error_task = SnowflakeOperator(
#                         task_id='insert_into_error_log',
#                         sql=f"""
#                             INSERT INTO ERROR_LOG (NAME, EMAIL, SSN, ERROR_MESSAGE)
#                             VALUES ('{record.NAME}', '{record.EMAIL}', '{record.SSN}', 'Invalid SSN length should be 4 digits')
#                         """,
#                         snowflake_conn_id="snow_sc",  # Connection ID defined in Airflow
#                         dag=dag,
#                     )
#                     insert_error_task.execute(snowflake_conn)
#             except ValidationError as e:
#                 for error in e.errors():
#                     field_name = error.get('loc')[-1]
#                     error_msg = error.get('msg')
#                     print(f"Error in {field_name}: {error_msg}")
#                     # Insert into ERROR_LOG table
#                     insert_error_task = SnowflakeOperator(
#                         task_id='insert_into_error_log',
#                         sql=f"""
#                             INSERT INTO ERROR_LOG (NAME, EMAIL, SSN, ERROR_MESSAGE)
#                             VALUES ('{row[0]}', '{row[1]}', '{row[2]}', '{error_msg}')
#                         """,
#                         snowflake_conn_id="snow_sc",  # Connection ID defined in Airflow
#                         dag=dag,
#                     )
#                     insert_error_task.execute(snowflake_conn)
#             except Exception as e:
#                 print(f"Error: {str(e)}")

#     snowflake_conn.close()



# # # Task to validate and load data using Pydantic
# def validate_and_load_data():
#     snowflake_conn = create_snowflake_connection()

#     # Input CSV file URL
#     csv_url = 'https://raw.githubusercontent.com/jcharishma/my.repo/master/sample_csv.csv'

#     # Fetch CSV data from the URL
#     response = requests.get(csv_url)
#     if response.status_code == 200:
#         csv_content = response.text
#         csv_lines = csv_content.split('\n')
#         header = csv_lines[0].strip().split(',')
#         for line in csv_lines[1:]:
#             line = line.strip()
#             if not line:
#                 continue
#             row = line.split(',')
#             if len(row) != len(header):
#                 # Invalid format, insert into ERROR_LOG table
#                 insert_error_task = SnowflakeOperator(
#                     task_id='insert_into_error_log',
#                     sql=f"""
#                         INSERT INTO ERROR_LOG (NAME, EMAIL, SSN, ERROR_MESSAGE)
#                         VALUES ('{row[0]}', '{row[1]}', '{row[2]}', 'Invalid CSV format')
#                     """,
#                     snowflake_conn_id="snow_sc",  # Connection ID 
#                     dag=dag,
#                 )
#                 insert_error_task.execute(snowflake_conn)
#                 continue

#             try:
#                 record = CSVRecord(NAME=row[0], EMAIL=row[1], SSN=row[2])
#                 if len(record.SSN) == 4:
#                     # Insert into SAMPLE_CSV table
#                     insert_task = SnowflakeOperator(
#                         task_id='insert_into_sample_csv',
#                         sql=f"""
#                             INSERT INTO SAMPLE_CSV (NAME, EMAIL, SSN)
#                             VALUES ('{record.NAME}', '{record.EMAIL}', '{record.SSN}')
#                         """,
#                         snowflake_conn_id="snow_sc",  # Connection ID 
#                         dag=dag,
#                     )
#                     insert_task.execute(snowflake_conn)
#                 else:
#                     # Insert into ERROR_LOG table
#                     insert_error_task = SnowflakeOperator(
#                         task_id='insert_into_error_log',
#                         sql=f"""
#                             INSERT INTO ERROR_LOG (NAME, EMAIL, SSN, ERROR_MESSAGE)
#                             VALUES ('{record.NAME}', '{record.EMAIL}', '{record.SSN}', 'Invalid SSN length should be 4 digits')
#                         """,
#                         snowflake_conn_id="snow_sc",  # Connection ID
#                         dag=dag,
#                     )
#                     insert_error_task.execute(snowflake_conn)
#             except ValidationError as e:
#                 for error in e.errors():
#                     field_name = error.get('loc')[-1]
#                     error_msg = error.get('msg')
#                     print(f"Error in {field_name}: {error_msg}")
#                     # Insert into ERROR_LOG table
#                     insert_error_task = SnowflakeOperator(
#                         task_id='insert_into_error_log',
#                         sql=f"""
#                             INSERT INTO ERROR_LOG (NAME, EMAIL, SSN, ERROR_MESSAGE)
#                             VALUES ('{row[0]}', '{row[1]}', '{row[2]}', '{error_msg}')
#                         """,
#                         snowflake_conn_id="snow_sc",  # Connection ID
#                         dag=dag,
#                     )
#                     insert_error_task.execute(snowflake_conn)
#             except Exception as e:
#                 print(f"Error: {str(e)}")

#     snowflake_conn.close()








# # Task to validate and load data using Pydantic
# def validate_and_load_data():
#     snowflake_conn = create_snowflake_connection()

#     # Input CSV file URL
#     csv_url = 'https://raw.githubusercontent.com/jcharishma/my.repo/master/sample_csv.csv'

#     # Fetch CSV data from the URL
#     response = requests.get(csv_url)
#     if response.status_code == 200:
#         csv_content = response.text
#         csv_lines = csv_content.split('\n')
#         csvreader = csv.reader(csv_lines, delimiter='\t')  # Use tab as the delimiter
#         next(csvreader)  # Skip the header row
#         for row in csvreader:
#             try:
#                 # Clean up the SSN field to remove commas
#                 ssn = row[2].replace(",", "")

#                 record = CSVRecord(NAME=row[0], EMAIL=row[1], SSN=ssn)

#                 # Check if SSN has more than 4 digits
#                 if len(record.SSN) > 4:
#                     # Insert into ERROR_LOG table
#                     insert_error_task = SnowflakeOperator(
#                         task_id='insert_into_error_log',
#                         sql=f"""
#                             INSERT INTO ERROR_LOG (NAME, EMAIL, SSN, ERROR_MESSAGE)
#                             VALUES ('{record.NAME}', '{record.EMAIL}', '{record.SSN}', 'Invalid SSN length should not be more than 4 digits')
#                         """,
#                         snowflake_conn_id="snow_sc",  # Connection ID defined in Airflow
#                         dag=dag,
#                     )
#                     insert_error_task.execute(snowflake_conn)
#                 else:
#                     # Insert into SAMPLE_CSV table
#                     insert_task = SnowflakeOperator(
#                         task_id='insert_into_sample_csv',
#                         sql=f"""
#                             INSERT INTO SAMPLE_CSV (NAME, EMAIL, SSN)
#                             VALUES ('{record.NAME}', '{record.EMAIL}', '{record.SSN}')
#                         """,
#                         snowflake_conn_id="snow_sc",  # Connection ID defined in Airflow
#                         dag=dag,
#                     )
#                     insert_task.execute(snowflake_conn)
#             except ValidationError as e:
#                 for error in e.errors():
#                     field_name = error.get('loc')[-1]
#                     error_msg = error.get('msg')
#                     print(f"Error in {field_name}: {error_msg}")
#             except Exception as e:
#                 print(f"Error: {str(e)}")








# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from datetime import datetime
# import requests
# import csv
# from pydantic import BaseModel, ValidationError

# # # Define Snowflake connection credentials
# # snowflake_credentials = {
# #     "account": "https://hzciyrm-kj91758.snowflakecomputing.com",
# #     "warehouse": "COMPUTE_WH",
# #     "database": "DEMO",
# #     "schema": "SC1",
# #     "username": "CJ",
# #     "password": "Cherry@2468"
# # }

# # Create a function to establish the Snowflake connection using SnowflakeHook
# def create_snowflake_connection():
#     hook = SnowflakeHook(snowflake_conn_id="snow_sc")  
#     conn = hook.get_conn()
#     return conn

# # Define the Pydantic model for CSV data
# class CSVRecord(BaseModel):
#     NAME: str
#     EMAIL: str
#     SSN: str

# # Task to read file from provided URL and display data
# def read_file_and_display_data():
#     # Input CSV file URL
#     csv_url = 'https://raw.githubusercontent.com/jcharishma/my.repo/master/sample_csv.csv'

#     # Fetch CSV data from the URL
#     response = requests.get(csv_url)
#     if response.status_code == 200:
#         csv_content = response.text
#         print("CSV Data:")
#         print(csv_content)
#         return csv_content
#     else:
#         raise Exception(f"Failed to fetch CSV: Status Code {response.status_code}")

# # Task to validate and load data using Pydantic
# def validate_and_load_data():
#     snowflake_conn = create_snowflake_connection()

#     # Input CSV file URL
#     csv_url = 'https://raw.githubusercontent.com/jcharishma/my.repo/master/sample_csv.csv'

#     # Fetch CSV data from the URL
#     response = requests.get(csv_url)
#     if response.status_code == 200:
#         csv_content = response.text
#         csv_lines = csv_content.split('\n')
#         csvreader = csv.DictReader(csv_lines)
#         for row in csvreader:
#             try:
#                 record = CSVRecord(**row)

#                 # Use SnowflakeOperator to insert data into Snowflake
#                 insert_task = SnowflakeOperator(
#                     task_id='insert_into_sample_csv',
#                     sql=f"""
#                         INSERT INTO SAMPLE_CSV (NAME, EMAIL, SSN)
#                         VALUES ('{record.NAME}', '{record.EMAIL}', '{record.SSN}')
#                     """,
#                     snowflake_conn_id="snow_sc",  # Connection ID defined in Airflow
#                     dag=dag,
#                 )
#                 insert_task.execute(snowflake_conn)
#             except ValidationError as e:
#                 for error in e.errors():
#                     field_name = error.get('loc')[-1]
#                     error_msg = error.get('msg')
#                     print(f"Error in {field_name}: {error_msg}")
#             except Exception as e:
#                 print(f"Error: {str(e)}")

#     snowflake_conn.close()

# # Airflow default arguments
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 9, 7),
#     'retries': 1,
#     'catchup': True,
# }

# # Create the DAG
# dag = DAG(
#     'csv_dag',
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
# )

# # Task to read file from provided URL and display data
# read_file_task = PythonOperator(
#     task_id='read_file_and_display_data',
#     python_callable=read_file_and_display_data,
#     dag=dag,
# )

# # Task to validate and load data using Pydantic
# validate_task = PythonOperator(
#     task_id='validate_and_load_data',
#     python_callable=validate_and_load_data,
#     dag=dag,
# )

# # Set task dependencies
# read_file_task >> validate_task




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

# def read_file_from_url(**kwargs):
#     url = "https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv" 
#     response = requests.get(url)
#     data = response.text
#     kwargs['ti'].xcom_push(key='csv_data', value=data)  
#     print(f"Read data from URL. Content: {data}")
# def load_csv_data(**kwargs):
#     ti = kwargs['ti']
#     data = ti.xcom_pull(key='csv_data', task_ids='read_file_task')
#     df = pd.read_csv(StringIO(data))

#     # Convert 'SSN' column to string data type
#     df['SSN'] = df['SSN'].astype(str)

#     # Create a new column 'Invalid_SSN' and set it to None for all rows
#     df['Invalid_SSN'] = None

#     # Check if SSN values are exactly 4 digits and update 'Invalid_SSN' column for invalid rows
#     invalid_rows = df['SSN'].str.len() != 4
#     df.loc[invalid_rows, 'Invalid_SSN'] = df.loc[invalid_rows, 'SSN']

#     valid_rows = df[~invalid_rows]

#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
#     table_name = 'demo.sc1.sample_csv'

#     if not valid_rows.empty:
#         snowflake_hook.insert_rows(table_name, valid_rows.values.tolist(), valid_rows.columns.tolist())
#         print(f"Data load completed successfully for {len(valid_rows)} rows.")

#     if not invalid_rows.all():
#         print(f"Error: {invalid_rows.sum()} rows have invalid SSN and were not loaded to Snowflake.")
#         print("Invalid Rows:")
#         print(df.loc[invalid_rows])


# def load_csv_data(**kwargs):
#     ti = kwargs['ti']
#     data = ti.xcom_pull(key='csv_data', task_ids='read_file_task')  
#     df = pd.read_csv(StringIO(data))
    
#     # Convert 'SSN' column to string data type
#     df['SSN'] = df['SSN'].astype(str)
    
#     valid_rows = df[df['SSN'].str.len() == 4]
#     invalid_rows = df[df['SSN'].str.len() != 4]

#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
#     table_name = 'demo.sc1.sample_csv' 
    
#     if not valid_rows.empty:
#         snowflake_hook.insert_rows(table_name, valid_rows.values.tolist(), valid_rows.columns.tolist())
#         print(f"Data load completed successfully for {len(valid_rows)} rows.")
    
#     if not invalid_rows.empty:
#         print(f"Error: {len(invalid_rows)} rows have invalid SSN and were not loaded to Snowflake.")
#         print("Invalid Rows:")
#         print(invalid_rows)

# with dag:
#     read_file_task = PythonOperator(
#         task_id='read_file_task',
#         python_callable=read_file_from_url,
#         provide_context=True,
#     )

#     load_csv_data_task = PythonOperator(
#         task_id='load_csv_data',
#         python_callable=load_csv_data,
#         provide_context=True,
#     )

# read_file_task >> load_csv_data_task


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

# def read_file_from_url(**kwargs):
#     url = "https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv" 
#     response = requests.get(url)
#     data = response.text
#     kwargs['ti'].xcom_push(key='csv_data', value=data)  # Push data as XCom variable
#     print(f"Read data from URL. Content: {data}")

# def load_csv_data(**kwargs):
#     ti = kwargs['ti']
#     data = ti.xcom_pull(key='csv_data', task_ids='read_file_task')  # Retrieve data from XCom
#     df = pd.read_csv(StringIO(data))
    
#     valid_rows = df[df['SSN'].astype(str).str.len() == 4]
#     invalid_rows = df[df['SSN'].astype(str).str.len() != 4]

#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
#     table_name = 'demo.sc1.sample_csv' 
    
#     if not valid_rows.empty:
#         snowflake_hook.insert_rows(table_name, valid_rows.values.tolist(), valid_rows.columns.tolist())
#         print(f"Data load completed successfully for {len(valid_rows)} rows.")
    
#     if not invalid_rows.empty:
#         print(f"Error: {len(invalid_rows)} rows have invalid SSN and were not loaded to Snowflake.")
#         print("Invalid Rows:")
#         print(invalid_rows)

# with dag:
#     read_file_task = PythonOperator(
#         task_id='read_file_task',
#         python_callable=read_file_from_url,
#         provide_context=True,
#     )

#     load_csv_data_task = PythonOperator(
#         task_id='load_csv_data',
#         python_callable=load_csv_data,
#         provide_context=True,
#     )

# read_file_task >> load_csv_data_task








# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import requests
# import csv

# # Snowflake connection ID
# # SNOWFLAKE_ID = 'snow_sc'

# default_args = {
#     'start_date': datetime(2023, 8, 31),
#     'catchup': False,
# }

# dag = DAG(
#     'charishma_csv_dag',
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
# )

# # Create a function to fetch data from the URL
# def fetch_data_from_url(**kwargs):
#     url = "https://github.com/jcharishma/my.repo/blob/master/sample_csv.csv"
#     response = requests.get(url)
#     response.raise_for_status()  

#     # Split the CSV data and skip the header row if present
#     data = []
#     for row in response.text.splitlines():
#         fields = row.split(',')
#         if len(fields) == 3:
#             name = fields[0]
#             email = fields[1]
#             ssn = fields[2]
            
#             # Check if SSN is exactly 4 digits
#             if ssn.isdigit() and len(ssn) == 4:
#                 data.append({'name': name, 'email': email, 'ssn': ssn})
#             else:
#                 print(f"Error: Invalid SSN detected in the CSV: {row}")

#     # Push the 'data' variable as an XCom value
#     kwargs['ti'].xcom_push(key='data', value=data)

# # Create the PythonOperator task to fetch data
# fetch_data_task = PythonOperator(
#     task_id='fetch_data',
#     python_callable=fetch_data_from_url,
#     provide_context=True,
#     dag=dag,
# )

# # Create a SnowflakeOperator task to load data into Snowflake
# snowflake_task = SnowflakeOperator(
#     task_id='load_data',
#     sql=f"COPY INTO sample_csv "
#     f"FROM 'https://raw.githubusercontent.com/jcharishma/my.repo/master/sample_csv.csv' "
#     f"FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);",
#     snowflake_conn_id='snow_sc',
#     autocommit=True,
#     depends_on_past=False,
#     dag=dag,
# )



# # Set up task dependencies
# fetch_data_task >> snowflake_task

# snowflake_task = SnowflakeOperator(
#     task_id='load_data',
#     sql=f"COPY INTO sample_csv "
#     f"FROM 'https://github.com/jcharishma/my.repo/blob/master/sample_csv.csv'"
#     f" FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);", 
#     snowflake_conn_id='snow_sc',
#     autocommit=True,
#     depends_on_past=False,
#     dag=dag,
# )



# from airflow import DAG
# from airflow.operators.python import PythonOperator, ShortCircuitOperator
# from datetime import datetime
# import pandas as pd
# import requests
# from io import StringIO
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# import os
# from airflow.models import Variable

# # global Snowflake connection ID
# SNOWFLAKE_CONN_ID = 'snow_sc'

# default_args = {
#     'start_date': datetime(2023, 8, 25),
#     'retries': 1,
# }

# # def check_env_variable(**kwargs):
# #     C_AIR_ENV = os.environ.get('C_AIR_ENV')
# #     if C_AIR_ENV == 'True':
# #         return True  # ShortCircuitOperator should return True to proceed with downstream tasks
# #     else:
# #         return False
# def check_env_variable(**kwargs):
#     if Variable.get('C_AIR_ENV') == 'True':
#         return True
#     else:
#         return False
# #     C_AIR_ENV = os.environ.get('C_AIR_ENV')
# #     print("C_AIR_ENV:", C_AIR_ENV) 
# #     print("Type of C_AIR_ENV:", type(C_AIR_ENV))  
# #     if C_AIR_ENV == 'True':
# #         print("Returning True")  
# #         return True
# #     else:
# #         print("Returning False") 
# #         return False



# def fetch_csv_and_upload(**kwargs):
#     url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
#     response = requests.get(url)
#     data = response.text
#     df = pd.read_csv(StringIO(data))
#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
#     table_name = 'air_local'
    
#     snowflake_hook.insert_rows(table_name, df.values.tolist(), df.columns.tolist())

# def filter_records(**kwargs):
#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
#     sql_task3 = """
#     SELECT *
#     FROM air_local
#     WHERE avail_seat_km_per_week > 698012498
#     """
    
#     result = snowflake_hook.get_records(sql_task3)
#     num_records = 10 if result else 5
    
#     return num_records

# def print_records(num_records, **kwargs):
#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
#     sql_task4 = f"""
#     SELECT *
#     FROM air_local
#     WHERE avail_seat_km_per_week > 698012498
#     LIMIT {num_records}
#     """
    
#     records = snowflake_hook.get_records(sql_task4)
#     print("Printing records:")
#     print(records)
    
# def final_task(**kwargs):
#     print("Processes completed successfully.")

# # Define the DAG
# with DAG('charishma_dags', schedule_interval=None, default_args=default_args) as dag:
#     check_env_task = ShortCircuitOperator(
#         task_id='check_env_variable',
#         python_callable=check_env_variable,
#         provide_context=True,
#     )

#     upload_data_task = PythonOperator(
#         task_id='fetch_csv_and_upload',
#         python_callable=fetch_csv_and_upload,
#         provide_context=True,
#     )
    
#     num_records_task = PythonOperator(
#         task_id='filter_records',
#         python_callable=filter_records,
#         provide_context=True,
#     )
    
#     print_records_task = PythonOperator(
#         task_id='print_records',
#         python_callable=print_records,
#         op_args=[num_records_task.output],  
#         provide_context=True,
#     )
    
#     final_print_task = PythonOperator(
#         task_id='final_print_task',
#         python_callable=final_task,
#         provide_context=True,
#     )

#     # Set task dependencies
#     check_env_task >> upload_data_task >> num_records_task >> print_records_task >> final_print_task





# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime
# import pandas as pd
# import requests
# from io import StringIO
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# import os

# # global Snowflake connection ID
# SNOWFLAKE_CONN_ID = 'snow_sc'

# default_args = {
#     'start_date': datetime(2023, 8, 25),
#     'retries': 1,
# }

# def check_env_variable(**kwargs):
#     c_air_env = os.environ.get('C_AIR_ENV')
#     if c_air_env == 'true':
#         return 'fetch_csv_and_upload'

# def fetch_csv_and_upload(**kwargs):
#     url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
#     response = requests.get(url)
#     data = response.text
#     df = pd.read_csv(StringIO(data))
    
#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
#     table_name = 'air_table'
    
#     snowflake_hook.insert_rows(table_name, df.values.tolist(), df.columns.tolist())

# def filter_records(**kwargs):
#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
#     sql_task3 = """
#     SELECT *
#     FROM air_table
#     WHERE avail_seat_km_per_week > 698012498
#     """
    
#     result = snowflake_hook.get_records(sql_task3)
#     num_records = 10 if result else 5
    
#     return num_records

# def print_records(num_records, **kwargs):
#     snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
#     sql_task4 = f"""
#     SELECT *
#     FROM air_table
#     WHERE avail_seat_km_per_week > 698012498
#     LIMIT {num_records}
#     """
    
#     records = snowflake_hook.get_records(sql_task4)
#     print("Printing records:")
#     print(records)
    
#     # Task 5: Print process completed
#     print("Process completed")

# with DAG('charishma_dags', schedule_interval=None, default_args=default_args) as dag:
#     check_env_task = PythonOperator(
#         task_id='check_env_variable',
#         python_callable=check_env_variable,
#         provide_context=True,
#     )

#     upload_data_task = PythonOperator(
#         task_id='fetch_csv_and_upload',
#         python_callable=fetch_csv_and_upload,
#         provide_context=True,
#     )
    
#     num_records_task = PythonOperator(
#         task_id='filter_records',
#         python_callable=filter_records,
#         provide_context=True,
#     )
    
#     print_records_task = PythonOperator(
#         task_id='print_records',
#         python_callable=print_records,
#         op_args=[num_records_task.output],  # Pass the output of num_records_task
#         provide_context=True,
#     )

#     check_env_task >> [upload_data_task, num_records_task, print_records_task]









# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime
# import pandas as pd
# import requests
# from io import StringIO
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# default_args = {
#     'start_date': datetime(2023, 8, 25),
#     'retries': 1,
# }

# def fetch_csv_and_upload(**kwargs):
#     try:
#         url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
#         response = requests.get(url)
        
#         if response.status_code == 200:
#             data = response.text
#             df = pd.read_csv(StringIO(data))
            
#             # Upload DataFrame to Snowflake
#             snowflake_hook = SnowflakeHook(snowflake_conn_id='snow_sc')
#             table_name = 'airflow_tasks'
#             snowflake_hook.insert_rows(table_name, df.values.tolist(), df.columns.tolist())
#             print("Data uploaded successfully.")
#         else:
#             print("Failed to fetch data from the URL. Status code:", response.status_code)
#     except Exception as e:
#         print("An error occurred:", str(e))

# with DAG('charishma_dags', schedule_interval=None, default_args=default_args) as dag:
#     upload_data_task = PythonOperator(
#         task_id='fetch_csv_and_upload',
#         python_callable=fetch_csv_and_upload,
#         provide_context=True,
#     )

#     upload_data_task






# from airflow import DAG
# from airflow.operators.python import BranchPythonOperator
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from datetime import datetime
# import os

# default_args = {
#     'start_date': datetime(2023, 8, 25),
#     'retries': 1,
# }

# def check_env_variable(**kwargs):
#     c_air_env = os.environ.get('C_AIR_ENV')
#     print(f"Value of C_AIR_ENV: {c_air_env}")
#     if c_air_env == 'true':
#         return 'load_data_task'
#     return None
# # def check_env_variable():
# #     if os.environ.get('C_AIR_ENV') == 'true':
# #         return 'load_data_task'
# #         return None
# with DAG('charishma_dags', schedule_interval=None, default_args=default_args) as dag:
#     check_env_task = BranchPythonOperator(
#         task_id='check_env_variable',
#         python_callable=check_env_variable,
#         provide_context=True,
#     )

#     load_data_task = SnowflakeOperator(
#         task_id='load_data_task',
#         sql=f"COPY INTO airflow_tasks "
#     f"FROM 'https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv'"
#     f" FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);", 
#         snowflake_conn_id='snow_sc',
#     )

#     check_env_task >> load_data_task


# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime



# # Step 2: Initiating the default_args
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2022, 11, 12),
# }

# # Define a Python function to be executed by the PythonOperator
# def print_hello():
#     print("Welcome to Charishma's dag!")

# # Step 3: Creating DAG Object
# dag = DAG(
#     dag_id='charishma_dag',
#     default_args=default_args,
#     schedule_interval='@once',  
#     catchup=False,
# )

# # Step 4: Creating task
# # Create a PythonOperator that will run the print_hello function
# task = PythonOperator(
#     task_id='print_welcome',
#     python_callable=print_hello,
#     dag=dag,
# )


# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from datetime import datetime

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 1, 1),
#     'retries': 1,
# }

# dag = DAG(
#     'charishma_dags',  
#     default_args=default_args,
#     schedule_interval='@once',
#     catchup=False,
# )

# sql_query = """
# SELECT max(id) AS max_id
# FROM table1
# """

# snowflake_task = SnowflakeOperator(
#     task_id='execute_snowflake_query',
#     sql=sql_query,
#     snowflake_conn_id='snow_conn',
#     autocommit=True,
#     dag=dag,
# )




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
