try:
    import logging
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd
    from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
    from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
def max_query(**context):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    result = hook.get_first("select max(age) from db1.schema1.users")
    logging.info("Number of rows in `abcd_db.public.test3`  - %s", result[0])

def count_query(**context):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    result = hook.get_first("select count(*) from db1.schema1.users)")
    logging.info("Number of rows in `abcd_db.public.test3`  - %s", result[0])

with DAG(
        dag_id="rohineesh_dag",
        schedule_interval="@hourly",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        },
        catchup=False) as f:

    query_table = PythonOperator(
        task_id="max_query",
        python_callable=max_query
    )

    query_table_1 = PythonOperator(
        task_id="count_query",
        python_callable=max_query
    )

query_table >> query_table_1
