import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from airflow.operators.python_operator import PythonOperator



default_args = dict(
    start_date= datetime(2021, 1, 1),
    owner="airflow",
    retries=1,
)


dag_args = dict(
    dag_id="shyanjali_dag",
    schedule_interval='@once',
    default_args=default_args,
    catchup=False,
)

def send_email_function():
    # Email configuration
    subject = 'Test'
    body = 'Hi This is a text email'
    sender_email = 'shyanjali.kantumuchu@exusia.com'
    receiver_emails = ['shyanjali.kantumuchu@exusia.com']
    # cc_emails = ['cc@example.com']
    # bcc_emails = ['bcc@example.com']
    smtp_server = 'smtp.exusia.com'
    smtp_port = 465  # Change to your SMTP server's port
    smtp_username = 'shyanjali.kantumuchu@exusia.com'
    smtp_password = 'Hatemaths123$'
    
    # Create the email message
    message = MIMEMultipart()
    message['Subject'] = subject
    message['From'] = sender_email
    message['To'] = ', '.join(receiver_emails)
    # message['Cc'] = ', '.join(cc_emails)
    # message['Bcc'] = ', '.join(bcc_emails)
    
    # Attach the email body
    message.attach(MIMEText(body, 'html'))  # You can change 'html' to 'plain' if using plain text
    
    try:
        # Connect to the SMTP server
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_username, smtp_password)
        
        # Send the email
        server.sendmail(sender_email, receiver_emails, message.as_string())
        
        # Close the SMTP server connection
        server.quit()
        print("Email sent successfully.")
    except Exception as e:
        print("Error sending email:", str(e))
with DAG(**dag_args) as dag:
# Create a PythonOperator to run the send_email_function
    send_email_task = PythonOperator(
        task_id='send_email',
        python_callable=send_email_function,
        dag=dag,  # Make sure to replace 'dag' with your DAG object
    )


send_email_task


# import logging
# from datetime import timedelta
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime
# import os
# from io import StringIO
# import pandas as pd
# import requests
# from airflow.models import Variable
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.operators.python import ShortCircuitOperator


# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)



# default_args = dict(
#     start_date= datetime(2021, 1, 1),
#     owner="airflow",
#     retries=1,
# )


# dag_args = dict(
#     dag_id="shyanjali_dag",
#     schedule_interval='@once',
#     default_args=default_args,
#     catchup=False,
# )


# def check_environment_variable():
#     # print(os.environ.get("AIRFLOW_SS"))
#     # Variable.set("AIRFLOW_LI", True)  # Change to False if needed
#     if Variable.get('AIRFLOW_LI') == True:
#         return True
#     else:
#         #stop dag
#         return False
    
# def fetch_csv_and_upload(**kwargs):
#     url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv"
#     response = requests.get(url)
#     data = response.text
#     df = pd.read_csv(StringIO(data))


#     # Upload DataFrame to Snowflake
#     snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_li')
#     # Replace with your Snowflake schema and table name
#     schema = 'PUBLIC'
#     table_name = 'AIRLINE'
#     connection = snowflake_hook.get_conn()
#     snowflake_hook.insert_rows(table_name, df.values.tolist())
#     connection.close()


# def get_data(**kwargs):
#     snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_li')
#     connection = snowflake_hook.get_conn()
#     create_table_query="SELECT * FROM AIRLINE WHERE avail_seat_km_per_week >698012498 LIMIT 10"
#     cursor = connection.cursor()
#     records = cursor.execute(create_table_query)
#     if records:
#         print("10 records")
#     else:
#         create_table_query="SELECT * FROM AIRLINE WHERE avail_seat_km_per_week LIMIT 5"
#         cursor = connection.cursor()
#         records = cursor.execute(create_table_query)
#         print("5 records")

#     for record in records:
#         print(record)

#     cursor.close()
#     connection.close()

# def print_success(**kwargs):
#     logging.info("Process Completed")


# with DAG(**dag_args) as dag:
#     # first task declaration
#     check_env_variable = ShortCircuitOperator(
#     task_id='check_env_variable',
#     python_callable=check_environment_variable,
#     provide_context=True,
#     op_kwargs={},
#     )

#     fetch_and_upload = PythonOperator(
#         task_id='fetch_and_upload',
#         python_callable=fetch_csv_and_upload,
#         provide_context=True,
#         op_kwargs={},# This is required to pass context to the function
#     )

#     get_data = PythonOperator(
#         task_id='get_data',
#         python_callable=get_data,
#         provide_context=True,
#         op_kwargs={},# This is required to pass context to the function
#     )

#     print_success = PythonOperator(
#         task_id='print_success',
#         python_callable=print_success,
#         provide_context=True,
#         op_kwargs={},# This is required to pass context to the function
#     )

#     check_env_variable >> fetch_and_upload >>get_data>>print_success

