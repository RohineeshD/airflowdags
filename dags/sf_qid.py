from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Function to execute the Snowflake query and store the result in df1
def execute_column_query(**kwargs):
    snowflake_conn_id = 'snow_id'  
    
    # Create a SnowflakeHook using the connection ID
    snowflake_hook = SnowflakeHook(snowflake_conn_id)
    
    # Snowflake query to retrieve column names
    column_query = """
    SELECT COLUMN_NAME 
    FROM SCHEMA1.DB1.INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_CATALOG='DB1' 
      AND TABLE_SCHEMA='SCHEMA1' 
      AND TABLE_NAME='USERS' 
    ORDER BY ORDINAL_POSITION;
    """
    
    # Execute the query and store the result in df1
    df1 = pd.read_sql(column_query, snowflake_hook.get_conn())
    
    # Store the last query ID in df1
    snowflake_query = "SELECT LAST_QUERY_ID() AS query_id;"
    df_last_query_id = pd.read_sql(snowflake_query, snowflake_hook.get_conn())
    df1['LAST_QUERY_ID'] = df_last_query_id.iloc[0]['QUERY_ID']
    
    # Push df1 to XCom for later use
    kwargs['ti'].xcom_push(key='df1', value=df1)

# Function to process df1 in a loop
def process_df1(**kwargs):
    ti = kwargs['ti']
    df1 = ti.xcom_pull(key='df1', task_ids='execute_query')  # Retrieve df1 from the previous task
    
    # Print "Standard Column names in the Table"
    print("Standard Column names in the Table")
    for column in df1['COLUMN_NAME']:
        print('Columns in table:', column)
    
    # Print the last query ID from df1
    print("Last Query ID:", df1['LAST_QUERY_ID'].iloc[0])
    
    # Load your uploaded file into a DataFrame
    df_uploaded = pd.read_csv('https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv')
    
    # Print "Column names from uploaded file" and map columns with the table
    print("Column names from uploaded file")
    for column_uploaded in df_uploaded.columns:
        if column_uploaded in df1['COLUMN_NAME'].values:
            print(f'Mapped Column in table: {column_uploaded} --> {df1[df1["COLUMN_NAME"] == column_uploaded].iloc[0]["COLUMN_NAME"]}')
        else:
            print(f'Column not found in the table: {column_uploaded}')

# Create the DAG
dag = DAG(
    'snow_id',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['snowflake']
)

# Execute Snowflake query and retrieve column names
'''snowflake_task = SnowflakeOperator(
    task_id='execute_snowflake_query',
    sql="SELECT LAST_QUERY_ID() AS query_id;",
    snowflake_conn_id='snow_id',
    dag=dag,
)'''

# Execute the column_query and store the result in df1
execute_query = PythonOperator(
    task_id='execute_query',
    python_callable=execute_column_query,
    provide_context=True,  # Required to pass 'kwargs'
    dag=dag,
)

# Process df1 in a loop
process_df1_task = PythonOperator(
    task_id='process_df1',
    python_callable=process_df1,
    provide_context=True,  # Required to pass 'kwargs'
    dag=dag,
)

# Set task dependencies
execute_query >> process_df1_task

if __name__ == "__main__":
    dag.cli()
