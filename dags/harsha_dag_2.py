from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
import pandas as pd

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'schedule_interval': None,  
    'catchup': False
}

dag = DAG(
    'load_snowflake',
    default_args=default_args,
    description='Load CSV data into Snowflake',
    catchup=False
)

# Define Snowflake connection ID from Airflow's Connection UI
snowflake_conn_id = 'air_conn'

def get_snowflake_hook(conn_id):
    return SnowflakeHook(snowflake_conn_id=conn_id)

def insert_data_to_snowflake(**kwargs):
    url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.text
        lines = data.strip().split('\n')[1:]
        snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

        # Truncate the table before loading new data
        truncate_query = "TRUNCATE TABLE airflow_tasks"
        snowflake_hook.run(truncate_query)
        
        for line in lines:
            values = line.split(',')
            query = f"""
                INSERT INTO airflow_tasks (airline, avail_seat_km_per_week, incidents_85_99, fatal_accidents_85_99, fatalities_85_99, incidents_00_14, fatal_accidents_00_14, fatalities_00_14)
                VALUES ('{values[0]}', '{values[1]}', '{values[2]}', '{values[3]}', '{values[4]}', '{values[5]}', '{values[6]}', '{values[7]}')
            """
            snowflake_hook.run(query)
            
        print("Data loaded into Snowflake successfully.")
    else:
        raise Exception(f"Failed to fetch data from URL. Status code: {response.status_code}")

# Define Snowflake target table
snowflake_table = 'bulk_table'

# Define the CSV URL
csv_url = 'https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv'

# Function to load CSV data into Snowflake
def copy_csv_to_snowflake():
    try:
        snowflake_hook = get_snowflake_hook(snowflake_conn_id)

        # Establish a Snowflake connection
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()

        # Create a Snowflake internal stage for the CSV file
        stage_name = 'csv_stage'
        create_stage_sql = f'''
        CREATE OR REPLACE STAGE {stage_name}
        FILE_FORMAT = (
            TYPE = 'CSV'
            SKIP_HEADER = 1
        );
        '''
        cursor.execute(create_stage_sql)

        # Download the CSV file to a local directory
        response = requests.get(csv_url)
        local_file_path = '/tmp/customers-100000.csv'
        with open(local_file_path, 'wb') as file:
            file.write(response.content)

        # Upload the CSV file to the Snowflake internal stage
        put_sql = f'''
        PUT 'file://{local_file_path}' @{stage_name}
        '''
        cursor.execute(put_sql)
        cursor.close()
        conn.close()

        # Snowflake COPY INTO command using the internal stage with error handling
        snowflake_hook.run( 
            f'''
            COPY INTO {snowflake_table}
            FROM @{stage_name}
            FILE_FORMAT = (
                TYPE = 'CSV'
                SKIP_HEADER = 1
            )
            ON_ERROR = 'CONTINUE';
            '''
        )

        # Drop the Snowflake internal stage after loading
        snowflake_hook.run(f'DROP STAGE IF EXISTS {stage_name}')

        print("Data loaded successfully")
        return True
    except Exception as e:
        print("Data loading failed -", str(e))
        return False

# Function to print CSV data before loading
def print_csv_data():
    response = requests.get(csv_url)
    data = response.text
    df = pd.read_csv(pd.compat.StringIO(data))
    print("CSV Data:")
    print(df.head())

insert_data_task = PythonOperator(
    task_id='load_data_task',
    python_callable=insert_data_to_snowflake,
    provide_context=True,
    dag=dag,
)


# Task to print CSV data before loading
print_csv_task = PythonOperator(
    task_id='print_csv_data_task',
    python_callable=print_csv_data,
    dag=dag
)

# Task to truncate the Snowflake table before loading
truncate_table_task = SnowflakeOperator(
    task_id='truncate_snowflake_table_task',
    sql=f'TRUNCATE TABLE {snowflake_table}',
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

# Task to call the load_csv_to_snowflake function
copy_csv_task = PythonOperator(
    task_id='load_csv_to_snowflake_task',
    python_callable=copy_csv_to_snowflake,
    dag=dag
)

# # Task to truncate the Snowflake table before loading
# truncate_table_task = SnowflakeOperator(
#     task_id='truncate_snowflake_table_task',
#     sql=f'TRUNCATE TABLE {snowflake_table}',
#     snowflake_conn_id=snowflake_conn_id,
#     dag=dag
# )

# Set task dependencies
insert_data_task >> print_csv_task >> truncate_table_task >> load_csv_task



# =========================================================working=================================================
# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# from datetime import datetime
# import requests

# # Define default_args for the DAG
# default_args = {
#     'owner': 'airflow',
#     'start_date': days_ago(1),
#     'schedule_interval': None,  
#     'catchup': False
# }

# dag = DAG(
#     'load_snowflake',
#     default_args=default_args,
#     description='Load CSV data into Snowflake',
#     catchup=False
# )

# # Define Snowflake connection ID from Airflow's Connection UI
# snowflake_conn_id = 'air_conn'

# def get_snowflake_hook(conn_id):
#     return SnowflakeHook(snowflake_conn_id=conn_id)

# # Define Snowflake target table
# snowflake_table = 'bulk_table'

# # Define the CSV URL
# csv_url = 'https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv'

# # Function to load CSV data into Snowflake
# def load_csv_to_snowflake():
#     try:
#         snowflake_hook = get_snowflake_hook(snowflake_conn_id)

#         # Establish a Snowflake connection
#         conn = snowflake_hook.get_conn()
#         cursor = conn.cursor()

#         # Create a Snowflake internal stage for the CSV file
#         stage_name = 'csv_stage'
#         create_stage_sql = f'''
#         CREATE OR REPLACE STAGE {stage_name}
#         FILE_FORMAT = (
#             TYPE = 'CSV'
#             SKIP_HEADER = 1

#         );
#         '''
#         cursor.execute(create_stage_sql)

#         # Download the CSV file to a local directory
#         response = requests.get(csv_url)
#         local_file_path = '/tmp/customers-100000.csv'
#         with open(local_file_path, 'wb') as file:
#             file.write(response.content)

#         # Upload the CSV file to the Snowflake internal stage
#         put_sql = f'''
#         PUT 'file://{local_file_path}' @{stage_name}
#         '''
#         cursor.execute(put_sql)
#         cursor.close()
#         conn.close()

#         # Snowflake COPY INTO command using the internal stage with error handling
#         snowflake_hook.run( 
#             f'''
#             COPY INTO {snowflake_table}
#             FROM @{stage_name}
#             FILE_FORMAT = (
#                 TYPE = 'CSV'
#                 SKIP_HEADER = 1

#             )
#             ON_ERROR = 'CONTINUE';
#             '''
#         )

#         # Drop the Snowflake internal stage after loading
#         snowflake_hook.run( f'DROP STAGE IF EXISTS {stage_name}')

#         print("Data loaded successfully")
#         return True
#     except Exception as e:
#         print("Data loading failed -", str(e))
#         return False

# # Task to call the load_csv_to_snowflake function
# load_csv_task = PythonOperator(
#     task_id='load_csv_to_snowflake_task',
#     python_callable=load_csv_to_snowflake,
#     dag=dag
# )


# if __name__ == "__main__":
#     dag.cli()


# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# from datetime import datetime
# import requests

# # Define default_args for the DAG
# default_args = {
#     'owner': 'airflow',
#     'start_date': days_ago(1),
#     'schedule_interval': None,  
#     'catchup': False
# }

# dag = DAG(
#     'load_snowflake',
#     default_args=default_args,
#     description='Load CSV data into Snowflake',
#     catchup=False
# )


# # Define Snowflake connection ID from Airflow's Connection UI
# # snowflake_conn_id = 'air_conn'
# def get_snowflake_hook(conn_id):
#     return SnowflakeHook(snowflake_conn_id=conn_id)

# # Define Snowflake target table
# snowflake_table = 'bulk_table'

# # Define the CSV URL
# csv_url = 'https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv'

# # Function to load CSV data into Snowflake
# def load_csv_to_snowflake():
#     try:
#         snowflake_hook = SnowflakeHook(snowflake_conn_id='air_conn')

#         # Establish a Snowflake connection
#         conn = snowflake_hook.get_conn()
#         cursor = conn.cursor()

#         # Create a Snowflake internal stage for the CSV file
#         stage_name = 'csv_stage'
#         create_stage_sql = f'''
#         CREATE OR REPLACE STAGE {stage_name}
#         FILE_FORMAT = (
#             TYPE = 'CSV'
#             SKIP_HEADER = 1
#         );
#         '''
#         cursor.execute(create_stage_sql)

#         # Download the CSV file to a local directory
#         response = requests.get(csv_url)
#         local_file_path = '/tmp/customers-100000.csv'
#         with open(local_file_path, 'wb') as file:
#             file.write(response.content)

#         # Upload the CSV file to the Snowflake internal stage
#         put_sql = f'''
#         PUT 'file://{local_file_path}' @{stage_name}
#         '''
#         cursor.execute(put_sql)

#         # Snowflake COPY INTO command using the internal stage
#         copy_into_sql = f'''
#         COPY INTO {snowflake_table}
#         FROM @{stage_name}
#         FILE_FORMAT = (
#             TYPE = 'CSV'
#             SKIP_HEADER = 1
#                FIELD_OPTIONALLY_ENCLOSED_BY = ''
#                FIELD_OPTIONALLY_ENCLOSED_BY = NONE
#                ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
#                SKIP_BYTE_ORDER_MARK = TRUE
#                STRIP_NULL_VALUES = FALSE
#                SKIP_UTF8_BOM = TRUE
#                ON_ERROR = 'CONTINUE'
# )

#         );
#         '''
#         cursor.execute(copy_into_sql)

#         # Drop the Snowflake internal stage after loading
#         drop_stage_sql = f'''
#         DROP STAGE IF EXISTS {stage_name}
#         '''
#         cursor.execute(drop_stage_sql)

#         cursor.close()
#         conn.close()

#         print("Data loaded successfully")
#         return True
#     except Exception as e:
#         print("Data loading failed -", str(e))
#         return False

# # Task to call the load_csv_to_snowflake function
# load_csv_task = PythonOperator(
#     task_id='load_csv_to_snowflake_task',
#     python_callable=load_csv_to_snowflake,
#     dag=dag
# )

# load_csv_task

# from datetime import datetime
# from airflow import DAG
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# import pandas as pd
# from sqlalchemy import create_engine

# # Define Snowflake connection ID from Airflow's Connection UI
# snowflake_conn_id = 'air_conn'  
# # Define Snowflake target table
# snowflake_table = 'bulk_table'
# # Define the CSV URL
# csv_url = "https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv"

# # Define default_args for the DAG
# default_args = {
#     'owner': 'airflow',
#     'start_date': days_ago(1),
#     'schedule_interval': None,  
#     'catchup': False
# }

# dag = DAG(
#     'load_snowflake',
#     default_args=default_args,
#     description='Load CSV data into Snowflake',
#     catchup=False
# )

# # Function to create a Snowflake SQLAlchemy engine
# def create_snowflake_engine():
#     # Define Snowflake connection parameters
#     snowflake_account = 'https://app.snowflake.com/smdjtrh/gc37630/w3xPDq9SaW27#query'
#     snowflake_username = 'harsha'
#     snowflake_password = 'Rama@342'
#     snowflake_database = 'exusia_db'
#     snowflake_schema = 'exusia_schema'
    
#     # Construct the Snowflake SQLAlchemy URL
#     engine_url = f'snowflake://{snowflake_username}:{snowflake_password}@{snowflake_account}/{snowflake_database}/{snowflake_schema}'
    
#     # Create the SQLAlchemy engine
#     engine = create_engine(engine_url)
    
#     return engine

# # Function to load CSV data into Snowflake
# def load_csv_to_snowflake():
#     try:
#         # Create the Snowflake SQLAlchemy engine
#         engine = create_snowflake_engine()
        
#         # Read the CSV file into a Pandas DataFrame
#         df = pd.read_csv(csv_url)

#         # df = df.replace('', '0')
    
#         for column in df.columns:
#             if df[column].dtype == int:
#                 df[column].fillna(0, inplace=True)  # Replace with 0 for integer columns
#             elif df[column].dtype == float:
#                 df[column].fillna(0.0, inplace=True)  # Replace with 0.0 for float columns
#             elif df[column].dtype == object:
#                 df[column].fillna('', inplace=True)  # Replace with an empty string for string columns
        
#         # Clean the data by replacing empty values with 0 (or any appropriate default)
#         # df = df.fillna(0)

        

#         # Use the SQLAlchemy engine to write the DataFrame to Snowflake
#         df.to_sql(snowflake_table, engine, if_exists='append', index=False)

#         print("Data loaded successfully")
#         return True
#     except Exception as e:
#         print("Data loading failed -", str(e))
#         return False

# # Create a PythonOperator to execute the CSV loading function
# load_csv_task = PythonOperator(
#     task_id='load_csv_to_snowflake_task',
#     python_callable=load_csv_to_snowflake,
#     dag=dag
# )

# load_csv_task

# if __name__ == "__main__":
#     dag.cli()


# from datetime import datetime
# from airflow import DAG
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# import pandas as pd
# from airflow.models import Variable

# dag = DAG(
#     'load_snowflake',
#     start_date=days_ago(1),
#     schedule_interval=None,
#     catchup=False
# )

# # Define Snowflake connection ID from Airflow's Connection UI
# snowflake_conn_id = 'air_conn'  
# # snowflake_conn_id = 'harsha_conn'

# # Define Snowflake target table
# snowflake_table = 'bulk_table'

# # Define a Variable for the URL
# url_variable = Variable.get("csv_file")
# # Define the CSV URL
# # csv_url = "https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv"


# # # Function to load CSV data into Snowflake
# def load_csv_to_snowflake():
#     try:
#         # Establish a Snowflake connection using SnowflakeHook
#         snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
#         conn = snowflake_hook.get_conn()

#         # Read the CSV file into a Pandas DataFrame
#         df = pd.read_csv(url_variable)

#         # Create SQLAlchemy engine from Snowflake connection
#         engine = conn.cursor().connection
#         engine.connect()

#         # Snowflake COPY INTO command using Pandas DataFrame
#         # with conn:
#         #     with conn.cursor() as cursor:
#         #         cursor.execute(f"TRUNCATE TABLE {snowflake_table}")  # Optionally truncate table
#         df.to_sql(snowflake_table, conn, if_exists='append', index=False)

#         print("Data loaded successfully")
#         return True
#     except Exception as e:
#         print("Data loading failed -", str(e))
#         return False

# load_csv_task = PythonOperator(
#     task_id='load_csv_to_snowflake_task',
#     python_callable=load_csv_to_snowflake,
#     dag=dag
# )

# load_csv_task


# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# from datetime import datetime
# import requests

# # Define your DAG
# dag = DAG(
#     'load_csv_to_snowflake',
#     start_date=days_ago(1),
#     schedule_interval=None,
#     catchup=False
# )

# # Define Snowflake connection ID from Airflow's Connection UI
# # snowflake_conn_id = 'air_conn'
# def get_snowflake_hook(conn_id):
#     return SnowflakeHook(snowflake_conn_id=conn_id)

# # Define Snowflake target table
# snowflake_table = 'bulk_table'

# # Define the CSV URL
# csv_url = 'https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv'

# # Function to load CSV data into Snowflake
# def load_csv_to_snowflake():
#     try:
#         snowflake_hook = SnowflakeHook(snowflake_conn_id='air_conn')

#         # Establish a Snowflake connection
#         conn = snowflake_hook.get_conn()
#         cursor = conn.cursor()

#         # Create a Snowflake internal stage for the CSV file
#         stage_name = 'csv_stage'
#         create_stage_sql = f'''
#         CREATE OR REPLACE STAGE {stage_name}
#         FILE_FORMAT = (
#             TYPE = 'CSV'
#             SKIP_HEADER = 1
#         );
#         '''
#         cursor.execute(create_stage_sql)

#         # Download the CSV file to a local directory
#         response = requests.get(csv_url)
#         local_file_path = '/tmp/customers-100000.csv'
#         with open(local_file_path, 'wb') as file:
#             file.write(response.content)

#         # Upload the CSV file to the Snowflake internal stage
#         put_sql = f'''
#         PUT 'file://{local_file_path}' @{stage_name}
#         '''
#         cursor.execute(put_sql)

#         # Snowflake COPY INTO command using the internal stage
#         copy_into_sql = f'''
#         COPY INTO {snowflake_table}
#         FROM @{stage_name}
#         FILE_FORMAT = (
#             TYPE = 'CSV'
#             SKIP_HEADER = 1
#                FIELD_OPTIONALLY_ENCLOSED_BY = ''
#                FIELD_OPTIONALLY_ENCLOSED_BY = NONE
#                ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
#                SKIP_BYTE_ORDER_MARK = TRUE
#                STRIP_NULL_VALUES = FALSE
#                SKIP_UTF8_BOM = TRUE
#                ON_ERROR = 'CONTINUE'
# )

#         );
#         '''
#         cursor.execute(copy_into_sql)

#         # Drop the Snowflake internal stage after loading
#         drop_stage_sql = f'''
#         DROP STAGE IF EXISTS {stage_name}
#         '''
#         cursor.execute(drop_stage_sql)

#         cursor.close()
#         conn.close()

#         print("Data loaded successfully")
#         return True
#     except Exception as e:
#         print("Data loading failed -", str(e))
#         return False

# # Task to call the load_csv_to_snowflake function
# load_csv_task = PythonOperator(
#     task_id='load_csv_to_snowflake_task',
#     python_callable=load_csv_to_snowflake,
#     dag=dag
# )

# load_csv_task



# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime
# import pandas as pd
# import numpy as np
# from sqlalchemy import create_engine
# import snowflake.connector

# # Define Snowflake connection parameters
# snowflake_username = 'devtest1'
# snowflake_password = '$Devtest123'
# snowflake_account = 'rsehyuo-ny51095'
# snowflake_database = 'DB1'
# snowflake_schema = 'SCHEMA1'
# snowflake_warehouse = 'WH1'

# # Define Snowflake target table
# snowflake_table = 'bulk_table'

# # Define the CSV URL
# csv_url = "https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv"

# # Function to load CSV data into Snowflake
# def load_csv_to_snowflake():
#     try:
#         # Create a Snowflake connection
#         conn = snowflake.connector.connect(
#             user=snowflake_username,
#             password=snowflake_password,
#             account=snowflake_account,
#             warehouse=snowflake_warehouse,
#             database=snowflake_database,
#             schema=snowflake_schema
#         )

#         # Read the CSV file into a Pandas DataFrame
#         df = pd.read_csv(csv_url)

#         # Handle empty strings by replacing them with None
#         df = df.applymap(lambda x: None if x == '' else x)

#         # Convert 'SubscriptionDate' column to datetime
#         df['SubscriptionDate'] = pd.to_datetime(df['SubscriptionDate'], errors='coerce')

#         # Check for errors and replace with None (null) values
#         df['SubscriptionDate'] = df['SubscriptionDate'].where(df['SubscriptionDate'].notnull(), None)

#         # Format 'SubscriptionDate' as a string in the "YYYY-MM-DD" format
#         df['SubscriptionDate'] = df['SubscriptionDate'].dt.strftime('%Y-%m-%d')

#         # Create SQLAlchemy engine from Snowflake connection
#         engine = create_engine(conn)

#         # Snowflake COPY INTO command using Pandas DataFrame
#         df.to_sql(snowflake_table, engine, if_exists='append', index=False)

#         print("Data loaded successfully")
#         return True
#     except Exception as e:
#         print("Data loading failed -", str(e))
#         return False

# # Define the default arguments for the DAG
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 9, 6),
#     'retries': 1,
# }

# # Create the Airflow DAG
# dag = DAG(
#     'load_csv_to_snowflake',
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
# )

# # Define the PythonOperator to execute the data loading function
# load_data_task = PythonOperator(
#     task_id='load_data_task',
#     python_callable=load_csv_to_snowflake,
#     dag=dag,
# )

# # Set task dependencies
# load_data_task







# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime
# import pandas as pd
# import numpy as np
# from sqlalchemy import create_engine
# import snowflake.connector

# # Define Snowflake connection parameters
# snowflake_username = 'harsha'
# snowflake_password = 'Rama@342'
# snowflake_account = 'smdjtrh.eu-west-1.snowflakecomputing.com'
# snowflake_database = 'exusia_db'
# snowflake_schema = 'exusia_schema'
# snowflake_warehouse = 'COMPUTE_WH'

# # Define Snowflake target table
# snowflake_table = 'bulk_table'

# # Define the CSV URL
# csv_url = "https://raw.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv"

# # Function to load CSV data into Snowflake
# def load_csv_to_snowflake():
#     try:
#         # Create a Snowflake connection
#         conn = snowflake.connector.connect(
#             user=snowflake_username,
#             password=snowflake_password,
#             account=snowflake_account,
#             warehouse=snowflake_warehouse,
#             database=snowflake_database,
#             schema=snowflake_schema
#         )

#         # Read the CSV file into a Pandas DataFrame
#         df = pd.read_csv(csv_url)

#         # Handle empty strings by replacing them with None
#         df = df.applymap(lambda x: None if x == '' else x)

#         # Validate and format date column
#         df['SubscriptionDate'] = pd.to_datetime(df['SubscriptionDate'], errors='coerce')
#         df['SubscriptionDate'] = df['SubscriptionDate'].dt.strftime('%Y-%m-%d')

#         # Create SQLAlchemy engine from Snowflake connection
#         engine = create_engine(conn)

#         # Snowflake COPY INTO command using Pandas DataFrame
#         df.to_sql(snowflake_table, engine, if_exists='append', index=False)

#         print("Data loaded successfully")
#         return True
#     except Exception as e:
#         print("Data loading failed -", str(e))
#         return False

# # Define the default arguments for the DAG
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 9, 6),
#     'retries': 1,
# }

# # Create the Airflow DAG
# dag = DAG(
#     'load_csv_to_snowflake',
#     default_args=default_args,
#     description='DAG to load CSV data into Snowflake',
#     schedule_interval=None,
#     catchup=False,
# )

# # Define the PythonOperator to execute the data loading function
# load_data_task = PythonOperator(
#     task_id='load_data_task',
#     python_callable=load_csv_to_snowflake,
#     dag=dag,
# )

# # Set task dependencies
# load_data_task


# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime
# import numpy as np
# from sqlalchemy import create_engine
# import pandas as pd

# # Define Snowflake connection parameters
# snowflake_username = 'harsha'
# snowflake_password = 'Rama@342'
# snowflake_account = 'https://app.snowflake.com/smdjtrh/gc37630/w3xPDq9SaW27#query'
# snowflake_database = 'exusia_db'
# snowflake_schema = 'exusia_schema'
# snowflake_warehouse = 'COMPUTE_WH'

# # Define Snowflake target table
# snowflake_table = 'bulk_table'

# # Define the CSV URL
# csv_url = "https://raw.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv"

# # Function to load CSV data into Snowflake
# def load_csv_to_snowflake():
#     try:
#         # Create a Snowflake connection string
#         snowflake_connection_string = (
#             f'snowflake://{snowflake_username}:{snowflake_password}@{snowflake_account}/'
#             f'?warehouse={snowflake_warehouse}&database={snowflake_database}&schema={snowflake_schema}'
#         )

#         # Create a SQLAlchemy engine
#         engine = create_engine(snowflake_connection_string)

#         # Read the CSV file into a Pandas DataFrame
#         df = pd.read_csv(csv_url)
#         df = df.replace('', np.nan)

#         # Load the DataFrame into Snowflake
#         df.to_sql(snowflake_table, engine, if_exists='append', index=False)

#         print("Data loaded successfully")
#         return True
#     except Exception as e:
#         print("Data loading failed -", str(e))
#         return False

# # Define the default arguments for the DAG
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 9, 6),  
#     'retries': 1,
# }

# # Create the Airflow DAG
# dag = DAG(
#     'load_csv_to_snowflake',
#     default_args=default_args,
#     description='DAG to load CSV data into Snowflake',
#     schedule_interval=None,  
#     catchup=False,  
# )

# # Define the PythonOperator to execute the data loading function
# load_data_task = PythonOperator(
#     task_id='load_data_task',
#     python_callable=load_csv_to_snowflake,  # Use the local function, not your_module
#     dag=dag,
# )

# # Set task dependencies
# # In this example, we are setting no dependencies, but you can specify them here.
# # For example, you can use `set_upstream` or `set_downstream` methods to define dependencies.

# if __name__ == "__main__":
#     dag.cli()



# from datetime import datetime
# from airflow import DAG
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# import pandas as pd

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
#         snowflake_hook = SnowflakeHook(snowflake_conn_id='s_h_connection')
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
#          snowflake_hook = SnowflakeHook(snowflake_conn_id='s_h_connection')
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

# # Define Snowflake connection ID from Airflow's Connection UI
# snowflake_conn_id = 'snowflake_creds'  

# # Define Snowflake target table
# snowflake_table = 'bulk_table'

# # Define the CSV URL
# csv_url = "https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv"


# # Function to load CSV data into Snowflake
# def load_csv_to_snowflake():
#     try:
#         # Establish a Snowflake connection using SnowflakeHook
#         snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
#         conn = snowflake_hook.get_conn()

#         # Read the CSV file into a Pandas DataFrame
#         df = pd.read_csv(csv_url)

#         # Create SQLAlchemy engine from Snowflake connection
#         engine = conn.cursor().connection
#         engine.connect()

#         # Snowflake COPY INTO command using Pandas DataFrame
#         # with conn:
#         #     with conn.cursor() as cursor:
#         #         cursor.execute(f"TRUNCATE TABLE {snowflake_table}")  # Optionally truncate table
#         df.to_sql(snowflake_table, conn, if_exists='append', index=False)

#         print("Data loaded successfully")
#         return True
#     except Exception as e:
#         print("Data loading failed -", str(e))
#         return False


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

# load_csv_task = PythonOperator(
#     task_id='load_csv_to_snowflake_task',
#     python_callable=load_csv_to_snowflake,
#     dag=dag
# )


# load_data_task >> check_load_status_task >> print_status_task >> load_csv_task



# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# from datetime import datetime
# import requests

# # Define your DAG
# dag = DAG(
#     'load_csv_to_snowflake',
#     start_date=days_ago(1),
#     schedule_interval=None,
#     catchup=False
# )

# # Define Snowflake connection ID from Airflow's Connection UI
# # snowflake_conn_id = 'snowflake_creds'
# def get_snowflake_hook(conn_id):
#     return SnowflakeHook(snowflake_conn_id=conn_id)

# # Define Snowflake target table
# snowflake_table = 'bulk_table'

# # Define the CSV URL
# csv_url = 'https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv'

# # Function to load CSV data into Snowflake
# def load_csv_to_snowflake():
#     try:
#         snowflake_hook = SnowflakeHook(snowflake_conn_id='s_h_connection')

#         # Establish a Snowflake connection
#         conn = snowflake_hook.get_conn()
#         cursor = conn.cursor()

#         # Create a Snowflake internal stage for the CSV file
#         stage_name = 'csv_stage'
#         create_stage_sql = f'''
#         CREATE OR REPLACE STAGE {stage_name}
#         FILE_FORMAT = (
#             TYPE = 'CSV'
#             SKIP_HEADER = 1
#         );
#         '''
#         cursor.execute(create_stage_sql)

#         # Download the CSV file to a local directory
#         response = requests.get(csv_url)
#         local_file_path = '/tmp/customers-100000.csv'
#         with open(local_file_path, 'wb') as file:
#             file.write(response.content)

#         # Upload the CSV file to the Snowflake internal stage
#         put_sql = f'''
#         PUT 'file://{local_file_path}' @{stage_name}
#         '''
#         cursor.execute(put_sql)

#         # Snowflake COPY INTO command using the internal stage
#         copy_into_sql = f'''
#         COPY INTO {snowflake_table}
#         FROM @{stage_name}
#         FILE_FORMAT = (
#             TYPE = 'CSV'
#             SKIP_HEADER = 1
#                FIELD_OPTIONALLY_ENCLOSED_BY = ''
#                FIELD_OPTIONALLY_ENCLOSED_BY = NONE
#                ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
#                SKIP_BYTE_ORDER_MARK = TRUE
#                STRIP_NULL_VALUES = FALSE
#                SKIP_UTF8_BOM = TRUE
#                ON_ERROR = 'CONTINUE'
# )

#         );
#         '''
#         cursor.execute(copy_into_sql)

#         # Drop the Snowflake internal stage after loading
#         drop_stage_sql = f'''
#         DROP STAGE IF EXISTS {stage_name}
#         '''
#         cursor.execute(drop_stage_sql)

#         cursor.close()
#         conn.close()

#         print("Data loaded successfully")
#         return True
#     except Exception as e:
#         print("Data loading failed -", str(e))
#         return False

# # Task to call the load_csv_to_snowflake function
# load_csv_task = PythonOperator(
#     task_id='load_csv_to_snowflake_task',
#     python_callable=load_csv_to_snowflake,
#     dag=dag
# )

# load_csv_task
# if __name__ == "__main__":
#     dag.cli()

# COPY INTO {snowflake_table}
# FROM @{stage_name}
# FILE_FORMAT = (
#     TYPE = 'CSV'
#     SKIP_HEADER = 1
#     FIELD_OPTIONALLY_ENCLOSED_BY = ''
#     FIELD_OPTIONALLY_ENCLOSED_BY = NONE
#     ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
#     SKIP_BYTE_ORDER_MARK = TRUE
#     STRIP_NULL_VALUES = FALSE
#     SKIP_UTF8_BOM = TRUE
#     ON_ERROR = 'CONTINUE'
# )

# from airflow import DAG
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# import pandas as pd

# # Define your DAG
# dag = DAG(
#     'load_csv_to_snowflake',
#     start_date=days_ago(1),
#     schedule_interval=None,
#     catchup=False
# )

# # Define Snowflake connection ID from Airflow's Connection UI
# snowflake_conn_id = 'snowflake_conn'  

# # Define Snowflake target table
# snowflake_table = 'bulk_table'

# # Define the CSV URL
# csv_url = 'https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv'

# # Function to load CSV data into Snowflake
# def load_csv_to_snowflake():
#     try:
#         # Establish a Snowflake connection using SnowflakeHook
#         snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
#         conn = snowflake_hook.get_conn()

#         # Read the CSV file into a Pandas DataFrame
#         df = pd.read_csv(csv_url)

#         # Create SQLAlchemy engine from Snowflake connection
#         engine = conn.cursor().connection
#         engine.connect()

#         # Snowflake COPY INTO command using Pandas DataFrame
#         # with conn:
#         #     with conn.cursor() as cursor:
#         #         cursor.execute(f"TRUNCATE TABLE {snowflake_table}")  # Optionally truncate table
#         df.to_sql(snowflake_table, conn, if_exists='append', index=False)

#         print("Data loaded successfully")
#         return True
#     except Exception as e:
#         print("Data loading failed -", str(e))
#         return False

# # Task to call the load_csv_to_snowflake function
# load_csv_task = PythonOperator(
#     task_id='load_csv_to_snowflake_task',
#     python_callable=load_csv_to_snowflake,
#     dag=dag
# )

# load_csv_task



# from airflow import DAG
# from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# from datetime import datetime
# import pandas as pd

# # Define your DAG
# dag = DAG(
#     'load_csv_to_snowflake',
#     start_date=days_ago(1),  
#     schedule_interval=None,  
#     catchup=False  
# )

# # Define Snowflake connection ID from Airflow's Connection UI
# snowflake_conn_id = 'snowflake_conn'

# # Define Snowflake target table
# snowflake_table = 'bulk_table'

# # Define the CSV URL
# csv_url = 'https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/customers/customers-100000.csv'

# # Function to load CSV data into Snowflake
# def load_csv_to_snowflake():
#     try:
#         snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

#         # Read the CSV file into a DataFrame
#         df = pd.read_csv(csv_url)

#         # Use SnowflakeHook to insert data into Snowflake table
#         conn = snowflake_hook.get_conn()
#         cursor = conn.cursor()

#         # Snowflake COPY INTO command
#         copy_into_sql = f'''
#         COPY INTO {snowflake_table}
#         FROM '{csv_url}'
#         FILE_FORMAT = (
#             TYPE = 'CSV'
#             SKIP_HEADER = 1
#         );
#         '''

#         # Execute the COPY INTO command
#         cursor.execute(copy_into_sql)
#         cursor.close()
#         conn.close()

#         print("Data loaded successfully")
#         return True
#     except Exception as e:
#         print("Data loading failed -", str(e))
#         return False

# # Task to call the load_csv_to_snowflake function
# load_csv_task = PythonOperator(
#     task_id='load_csv_to_snowflake_task',
#     python_callable=load_csv_to_snowflake,
#     dag=dag
# )

# # if __name__ == "__main__":
# #     dag.cli()




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
#         snowflake_hook = SnowflakeHook(snowflake_conn_id='air_conn')
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
#          snowflake_hook = SnowflakeHook(snowflake_conn_id='air_conn')
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
