from io import StringIO
from airflow import DAG
import os
import logging
import requests
# Importing datetime and timedelta modules for scheduling the DAGs
from datetime import timedelta, datetime
# Importing operators 
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas

default_args = {
        'owner' : 'airflow',
        'start_date' :days_ago(2)
    }

def m1():
    sf_hook = SnowflakeHook(snowflake_conn_id='snow_id')
    conn = sf_hook.get_conn()
    cur = conn.cursor();
    df1=cur.execute("select COLUMN_NAME from information_schema.columns where TABLE_CATALOG='DB1' and TABLE_SCHEMA='SCHEMA1' and table_name='USERS' order by ORDINAL_POSITION;")
    df2 = cur.execute('SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));')
    
    choices=df2.fecthall();
    cur.close();

    print("Standard Column names in the Table")
    # st.write(choices)
    for i in choices:
        print('Columns in table:', i)

    print("Column names from uploaded file")
    for i in choices:
        print('Mapp Columns with table:', df.columns)


with DAG(dag_id='bh_sf_testing',
        default_args=default_args,
        schedule_interval='@once', 
        catchup=False
    ) as dag:

    execute = PythonOperator(
        task_id="executeIT",
        python_callable=m1
    )

execute
