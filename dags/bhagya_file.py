# Step 1: Importing Modules
# To initiate the DAG Object
from airflow import DAG
# Importing datetime and timedelta modules for scheduling the DAGs
from datetime import timedelta, datetime
# Importing operators 
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

# Step 2: Initiating the default_args
default_args = {
        'owner' : 'airflow',
        'start_date' :datetime(2023, 08, 23)
}


# Define the SQL query you want to execute in Snowflake
sql_query = """
SELECT max(id) FROM emp_data;
"""

# Step 3: Creating DAG Object
dag = DAG(dag_id='bhagya_dag',
        default_args=default_args,
        schedule_interval='@once', 
        catchup=False
    )

# Step 4: Creating task
# Creating first task
start = DummyOperator(task_id = 'start', dag = dag)


# Create a SnowflakeOperator task
snowflake_task = SnowflakeOperator(
    task_id='execute_snowflake_query',
    sql=sql_query,
    snowflake_conn_id='sf_bhagya',  # Set this to your Snowflake connection ID
    autocommit=True,  # Set autocommit to True if needed
    dag=dag
)

# Creating second task 
end = DummyOperator(task_id = 'end', dag = dag)

 # Step 5: Setting up dependencies 
snowflake_task
