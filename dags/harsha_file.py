from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'harsha_dag',  
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
)

sql_query = """
"SELECT * FROM patients WHERE status = 'Recovered'
"""

snowflake_task = SnowflakeOperator(
    task_id='execute_snowflake_query',
    sql=sql_query,
    snowflake_conn_id='snowflake_conn',
    autocommit=True,
    dag=dag,
)
