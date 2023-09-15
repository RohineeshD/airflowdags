import pandas as pd
from pydantic import BaseModel, ValidationError, validator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
from io import StringIO
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from airflow.hooks.base_hook import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

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

# Define Pydantic model for validation
class CsvRow(BaseModel):
    NAME: str
    EMAIL: str
    SSN: int
    @validator('SSN')
    def validate_id(cls, SSN):
        if not (1000 <= SSN <= 9999):
            raise ValueError('ID must be a 4-digit integer')
        return SSN

def fetch_and_validate_csv():
    valid_rows=[]
    
    # Replace with your Snowflake schema and table name
    try:
        # Fetch data from CSV URL
        response = requests.get(CSV_URL)
        response.raise_for_status()

        # Read CSV data into a DataFrame
        df = pd.read_csv(StringIO(response.text))
        print(df)
        # Iterate through rows and validate each one
        errors = []
        for index, row in df.iterrows():
            try:
                validated_row=CsvRow(**row.to_dict())
                print(validated_row)
                valid_rows.append((validated_row.NAME, validated_row.EMAIL, validated_row.SSN))
            except ValidationError as e:
                # Record validation errors and add them to the "error" column
                error_message = f"CHECK SSN IT SHOULD HAVE 4 DIGIT NUMBER: {str(e)}"
                errors.append(error_message)
                df.at[index, 'error'] = error_message
        # Add a new column to the DataFrame if not already present
        if 'error' not in df.columns:
            df['error'] = "--"
        
        print(df)
        # Check for NaN values in the entire DataFrame
        has_nan = df.isnull().values.any()
        
        # If there are NaN values, replace them with 0
        if has_nan:
            df.fillna(0, inplace=True)

        # df.to_csv('/tmp/data.csv', index=False)
        
        # snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_li')
        # schema = 'PUBLIC'
        # table_name = 'SAMPLE_CSV_ERROR'
        # connection = snowflake_hook.get_conn()
        # snowflake_hook.insert_rows(table_name, df.values.tolist())
        # connection.close()
        # print(f"CSV at {CSV_URL} has been validated successfully.")
    
    except Exception as e:
        print(f"Error: {str(e)}")
    
    return df
       
        
    # snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_li')
    # schema = 'PUBLIC'
    # table_name = 'SAMPLE_CSV'
    # connection = snowflake_hook.get_conn()
    # snowflake_hook.insert_rows(table_name, valid_rows)
    # connection.close()
    

def send_email(**kwargs):

    ti = context['ti']
    received_dataframe = ti.xcom_pull(task_ids='create_dataframe_task')
    received_dataframe.to_csv('/tmp/data.csv', index=False)
    email_content = "Errors in csv uploaded"
    # Use BaseHook to get the connection
    connection = BaseHook.get_connection(connection_id)

    # Access connection details
    smtp_server = connection.host
    smtp_port = connection.port
    smtp_username = connection.login
    smtp_password = connection.password
    sender_email = 'shyanjali.kantumuchu@exusia.com'

    recipient_email = ['shyanjali.kantumuchu@exusia.com']

    # Email details
    email_subject = subject
    # status =  email_content

    for r_email in recipient_email:
        # Create the email message
        message = MIMEMultipart()
        message['From'] = sender_email
        message['To'] = r_email
        message['Subject'] = email_subject
        # Add text to the email (optional)
        msg.attach(MIMEText('Please find the attached CSV file.', 'plain'))
    
        # Attach the CSV file
        csv_file_path = '/tmp/data.csv'
        with open(csv_file_path, 'rb') as file:
            csv_attachment = MIMEApplication(file.read(), Name=os.path.basename(csv_file_path))
        csv_attachment['Content-Disposition'] = f'attachment; filename="{os.path.basename(csv_file_path)}"'
        msg.attach(csv_attachment)
    
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

    
    
validate_csv = PythonOperator(
    task_id='validate_csv',
    python_callable=fetch_and_validate_csv,
    provide_context=True,
    dag=dag,
)

send_email_task = PythonOperator(
    task_id='send_email_task',
    python_callable=send_email,
    dag=dag,
)

validate_csv>>send_email_task







# --------------------------------------------------
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
# Add text to the email (optional)
        # msg.attach(MIMEText('Please find the attached CSV file.', 'plain'))
    
        # # Attach the CSV file
        # csv_file_path = '/path/to/your/csv/file.csv'
        # with open(csv_file_path, 'rb') as file:
        #     csv_attachment = MIMEApplication(file.read(), Name=os.path.basename(csv_file_path))
        # csv_attachment['Content-Disposition'] = f'attachment; filename="{os.path.basename(csv_file_path)}"'
        # msg.attach(csv_attachment)
    
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
