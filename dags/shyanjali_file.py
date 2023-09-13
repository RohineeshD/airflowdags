from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
import requests
import pandas as pd
from pydantic import BaseModel, ValidationError

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CsvDataValidationOperator(BaseOperator):
    @apply_defaults
    def __init__(self, csv_url, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.csv_url = csv_url

    def execute(self, context):
        # Fetch data from CSV URL
        response = requests.get(self.csv_url)
        response.raise_for_status()
        
        # Read CSV data into a DataFrame
        try:
            df = pd.read_csv(response.text)
        except pd.errors.EmptyDataError:
            self.log.error(f"CSV at {self.csv_url} is empty.")
            return
        
        # Define Pydantic model for validation
        class CsvRow(BaseModel):
            NAME: str
            EMAIL: str
            SSN: int
        
        # Iterate through rows and validate each one
        for index, row in df.iterrows():
            try:
                CsvRow(**row.to_dict())
            except ValidationError as e:
                self.log.error(f"Validation error in row {index}: {e}")
                # You can choose to raise an exception or handle errors as needed

        self.log.info(f"CSV at {self.csv_url} has been validated successfully.")

# Define your CSV URL
CSV_URL = 'https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'shyanjali_dag',
    default_args=default_args,
    schedule_interval=None,  # You can set a schedule as needed
    catchup=False,
)

start = DummyOperator(task_id='start', dag=dag)

validate_csv = CsvDataValidationOperator(
    task_id='validate_csv',
    csv_url=CSV_URL,
    dag=dag,
)

start >> validate_csv






# from airflow import DAG
# from airflow.hooks.base_hook import BaseHook
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime
# import smtplib
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from io import StringIO
# import pandas as pd
# import requests

# default_args = dict(
#     start_date= datetime(2021, 1, 1),
#     owner="airflow",
#     retries=1,
# )

# dag_args = dict(
#     dag_id="shyanjali_send_email",
#     schedule_interval='@once',
#     default_args=default_args,
#     catchup=False,
# )

# def fetch_csv_and_upload(**kwargs):
#     try:
#         url = "https://raw.githubusercontent.com/cs109/2014_data/master/countries.csv"
#         response = requests.get(url)
#         data = response.text
#         df = pd.read_csv(StringIO(data))
#         # Upload DataFrame to Snowflake
#         snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_li')
#         # Replace with your Snowflake schema and table name
#         schema = 'PUBLIC'
#         table_name = 'STAGING_TABL'
#         connection = snowflake_hook.get_conn()
#         snowflake_hook.insert_rows(table_name, df.values.tolist())
#         connection.close()
#         status =" DATA ADDED IN SNOWFLAKE STAGING TABLE "
#     except Exception as e:
#         status = f"Error loading CSV into Snowflake: {str(e)}"
#     return status


# # Specify the connection ID you want to retrieve details for
# connection_id = 'EMAIL_LI'

# # Function to send an email using SMTP connection details
# def send_email(**kwargs):

#     ti = kwargs['ti']  # Get the TaskInstance
#     status = ti.xcom_pull(task_ids='fetch_and_upload')  # Retrieve the status from Task 1 XCom
#     if status and status.startswith("Error"):
#         subject = 'CSV Load Failed'
#     else:
#         subject = 'CSV Load Success'

#     email_content = f"CSV Load Status: {status}"
#     # Use BaseHook to get the connection
#     connection = BaseHook.get_connection(connection_id)

#     # Access connection details
#     smtp_server = connection.host
#     smtp_port = connection.port
#     smtp_username = connection.login
#     smtp_password = connection.password
#     sender_email = 'shyanjali.kantumuchu@exusia.com'
#     recipient_email = ['shyanjali.kantumuchu@exusia.com','harsha.vardhan@exusia.com']

#     # Email details
#     email_subject = subject
#     # status =  email_content

#     email_body = f"""
#     <html>
#     <head>
#         <style>
#             /* Define styles for the box */
#             .status-box {{
#                 border: 2px solid #007bff;
#                 padding: 10px;
#                 background-color: #f0faff;
#             }}
    
#             /* Define styles for the heading */
#             h1 {{
#                 color: #007bff;
#             }}
#         </style>
#     </head>
#     <body>
#         <h1>Airflow Status Notification</h1>
#         <div class="status-box">
#             <p>Hello,</p>
#             <p>This is an Airflow status notification email.</p>
#             <p>Status: <strong>{email_content}</strong></p>
#         </div>
#     </body>
#     </html>
#     """

#     for r_email in recipient_email:
#         # Create the email message
#         message = MIMEMultipart()
#         message['From'] = sender_email
#         message['To'] = r_email
#         message['Subject'] = email_subject
#         message.attach(MIMEText(email_body, 'html'))
    
#         # Send the email
#         try:
#             server = smtplib.SMTP(smtp_server, smtp_port)
#             server.starttls()
#             server.login(smtp_username, smtp_password)
#             server.sendmail(sender_email, recipient_email, message.as_string())
#             server.quit()
#             print("Email sent successfully!")
#         except Exception as e:
#             print(f"Failed to send email: {str(e)}")

# # Task to send the email using the defined function
# with DAG(**dag_args) as dag:
#     # first task declaration
#     fetch_and_upload = PythonOperator(
#         task_id='fetch_and_upload',
#         python_callable=fetch_csv_and_upload,
#         provide_context=True,
#         op_kwargs={},# This is required to pass context to the function
#     )
# send_email_task = PythonOperator(
#     task_id='send_email_task',
#     python_callable=send_email,
#     dag=dag,
# )

# fetch_and_upload >>send_email_task
