from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from datetime import datetime

#default_args
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'sf_id',
    default_args=default_args,
    schedule_interval=None,  
    catchup=False, 
    tags=['snowflake']
)

# Snowflake query 
snowflake_query = """
SELECT LAST_QUERY_ID() AS query_id;
"""

# SnowflakeOperator to execute the query
snowflake_task = SnowflakeOperator(
    task_id='execute_snowflake_query',
    sql=snowflake_query,
    snowflake_conn_id='snow_id',
    dag=dag,
)

#task dependencies
snowflake_task

if __name__ == "__main__":
    dag.cli()
