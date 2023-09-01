from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from io import StringIO
import pandas as pd
import requests

default_args = dict(
    start_date= datetime(2021, 1, 1),
    owner="airflow",
    retries=1,
)

dag_args = dict(
    dag_id="shyanjali_send_email",
    schedule_interval='@once',
    default_args=default_args,
    catchup=False,
)

def fetch_csv_and_upload(**kwargs):
    try:
        url = "https://raw.githubusercontent.com/cs109/2014_data/master/countries.csv"
        response = requests.get(url)
        data = response.text
        df = pd.read_csv(StringIO(data))
        # Upload DataFrame to Snowflake
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_li')
        # Replace with your Snowflake schema and table name
        schema = 'PUBLIC'
        table_name = 'STAGING_TABL'
        connection = snowflake_hook.get_conn()
        snowflake_hook.insert_rows(table_name, df.values.tolist())
        connection.close()
        status =" DATA ADDED IN SNOWFLAKE STAGING TABLE "
    except Exception as e:
        status = f"Error loading CSV into Snowflake: {str(e)}"
        print(f"Error loading CSV into Snowflake: {str(e)}")
    return status


# Specify the connection ID you want to retrieve details for
connection_id = 'EMAIL_LI'

# Function to send an email using SMTP connection details
def send_email(**kwargs):

    ti = kwargs['ti']  # Get the TaskInstance
    status = ti.xcom_pull(task_ids='load_csv_into_snowflake')  # Retrieve the status from Task 1 XCom
    print("SSSSSSS",status)
    if status and status.startswith("Error"):
        subject = 'CSV Load Failed'
    else:
        subject = 'CSV Load Success'

    email_content = f"CSV Load Status: {status}"
    # Use BaseHook to get the connection
    connection = BaseHook.get_connection(connection_id)

    # Access connection details
    smtp_server = connection.host
    smtp_port = connection.port
    smtp_username = connection.login
    smtp_password = connection.password
    sender_email = 'shyanjali.kantumuchu@exusia.com'
    recipient_email = 'shyanjali.kantumuchu@exusia.com'

    # Email details
    email_subject = subject
    email_body =  email_content

    # Create the email message
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = recipient_email
    message['Subject'] = email_subject
    message.attach(MIMEText(email_body, 'plain'))

    # Send the email
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.sendmail(sender_email, recipient_email, message.as_string())
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {str(e)}")

# Define your DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2023, 9, 1),
#     'retries': 1,
# }

# dag = DAG(
#     'shyanjali_send_email',
#     default_args=default_args,
#     schedule_interval='@once',  # Set your desired schedule interval
#     catchup=False,
# )

# Task to send the email using the defined function
with DAG(**dag_args) as dag:
    # first task declaration
    fetch_and_upload = PythonOperator(
        task_id='fetch_and_upload',
        python_callable=fetch_csv_and_upload,
        provide_context=True,
        op_kwargs={},# This is required to pass context to the function
    )
send_email_task = PythonOperator(
    task_id='send_email_task',
    python_callable=send_email,
    dag=dag,
)

fetch_and_upload >>send_email_task
