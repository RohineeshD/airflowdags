from datetime import datetime
from airflow import DAG
from datetime import timedelta, datetime
import requests
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from io import StringIO
import pandas as pd
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.email_operator import EmailOperator


# Define default arguments for the DAG
default_args = {
    'owner': 'exusia_team',
    'start_date': datetime(2023, 8, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

snowflake_conn_id = 'snowflake_connection'

sql_query = """

INSERT INTO main_vikas(COUNTRY, REGION)
SELECT COUNTRY, REGION
FROM stag_vikas;

"""


def check_data_loading():
    
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_connection')
    schema = 'af_sch'
    table_name = 'stag_vikas'
    connection = snowflake_hook.get_conn()
    query = "SELECT COUNT(*) FROM main_vikas;"
    result = snowflake_hook.get_first(query)
    
    if result[0] > 0:
        print("data loading is Successful")
    else:
        print("Data not loaded")


with DAG('dag2_vik', default_args=default_args, schedule_interval=None) as dag:
    
       
    stag_to_main = SnowflakeOperator(
    task_id='stag_to_main',
    sql=sql_query,
    snowflake_conn_id='snowflake_connection',  # Connection ID configured in Airflow
    autocommit=True,  # Set to True to automatically commit the transaction
    database='airflow_data',
    warehouse='COMPUTE_WH',
    dag=dag,
    )

    check_data_loading = PythonOperator(
    task_id='check_data_loading',
    python_callable=check_data_loading,
    dag=dag,
    )

    email_task = EmailOperator(
    task_id='email_task',
    to='vikasdeep.singh@exausia.com',
    subject='Airflow Email Example',
    html_content='<p>This is the HTML body of the email.</p>',
    mime_charset='utf-8',
    dag=dag,
    smtp_host='smtp.gmail.com',  # Replace with your SMTP server address
    smtp_starttls=True,               # Set to True if your SMTP server requires STARTTLS
    smtp_ssl=False,                   # Set to True if your SMTP server uses SSL
    smtp_user='vikasdeeps319@example.com',  # Replace with your email address
    smtp_password='Vikas@123',  # Replace with your email password
    smtp_port=587,  # Use the appropriate SMTP port
    smtp_mail_from='vikasdeeps319@example.com'  # Replace with your email address
    )

# Setting up task dependencies 
stag_to_main  >>  check_data_loading  >>  email_task
