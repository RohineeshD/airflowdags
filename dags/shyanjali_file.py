from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import datetime

# Define a function to calculate the sum of the first 10 even numbers
def calculate_sum_of_even_numbers():
    even_numbers = [2 * i for i in range(1, 11)]
    result = sum(even_numbers)
    return result

# Define a function to print the result
def print_result(result):
    print(f"The sum of the first 10 even numbers is: {result}")

# Define the default_args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Create a DAG instance
with DAG(
    'shyanjali_dag',
    default_args=default_args,
    description='Example DAG with TaskGroup',
    schedule_interval=None,  # Set the schedule interval as needed
    catchup=False,  # Prevent catching up on historical runs
    tags=['example'],
) as f:

    # Task 1: Start
    start_task = DummyOperator(
        task_id='start_task',
        dag=dag,
    )
    
    # TaskGroup for Task 2 and Task 3
    with TaskGroup('even_number_tasks') as even_number_tasks:
    
        # Task 2: Calculate the sum of the first 10 even numbers
        calculate_sum_task = PythonOperator(
            task_id='task_2',
            python_callable=calculate_sum_of_even_numbers,
            dag=dag,
        )
    
        # Task 3: Print the result
        print_result_task = PythonOperator(
            task_id='task_3',
            python_callable=print_result,
            op_args=[calculate_sum_task.output],  # Pass the output from Task 2 to Task 3
            dag=dag,
        )
    
    # Task 4: End
    end_task = DummyOperator(
        task_id='end_task',
        dag=dag,
    )
    
    # Define task dependencies
    start_task >> even_number_tasks >> end_task



# ---------------------
# from airflow import DAG
# from airflow.operators.dummy import DummyOperator
# from airflow.operators.python import PythonOperator
# from airflow.utils.task_group import TaskGroup
# import pandas as pd
# import requests
# from io import StringIO 
# from datetime import datetime# Required for Python 3
# loaded_df = None  # Define a global variable to store the loaded DataFrame

# def start_task():
#     print("Starting Task 1")
#     # Perform any necessary initialization
#     pass

# def read_csv_from_url():
#     global loaded_df  # Access the global variable
#     print("Starting Task 2")
#     # Define the URL of the CSV file
#     csv_url = 'https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv'  # Replace with the actual URL

#     try:
#         # Fetch CSV data from the URL
#         response = requests.get(csv_url)

#         # Check if the request was successful
#         if response.status_code == 200:
#             csv_data = response.text

#             # Convert the CSV data to a pandas DataFrame
#             loaded_df = pd.read_csv(StringIO(csv_data))  # Assign the loaded DataFrame to the global variable
#             print("Loaded CSV data")
#             print(loaded_df)

#         else:
#             print(f"Failed to fetch CSV from URL. Status code: {response.status_code}")

#     except Exception as e:
#         print(f"An error occurred: {str(e)}")

# def validate_csv():
#     global loaded_df  # Access the global variable
#     print("Starting Task 3 - Validation")

#     # Perform validation logic on the loaded DataFrame
#     # Replace this with your actual validation logic
#     print(loaded_df)
#     if loaded_df is not None:
#         validation_passed = True
#         # Example validation: Check if 'column_name' exists in the DataFrame
#         if 'column_name' not in loaded_df.columns:
#             validation_passed = False
#     else:
#         validation_passed = False

#     print(f"CSV Validation Result: {validation_passed}")

# # def start_task():
# #     print("Starting Task 1")
# #     # Perform any necessary initialization
# #     pass

# # def read_csv_from_url(**kwargs):
# #     print("Starting Task 2")
# #     # Define the URL of the CSV file
# #     csv_url = 'https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv' # Replace with the actual URL

# #     try:
# #         # Fetch CSV data from the URL
# #         response = requests.get(csv_url)

# #         # Check if the request was successful
# #         if response.status_code == 200:
# #             csv_data = response.text

# #             # Convert the CSV data to a pandas DataFrame
# #             df = pd.read_csv(StringIO(csv_data))

# #             # Set the loaded DataFrame as an XCom variable
# #             kwargs['ti'].xcom_push(key='loaded_df', value=df)
# #             print("Loaded CSV data")

# #         else:
# #             print(f"Failed to fetch CSV from URL. Status code: {response.status_code}")

# #     except Exception as e:
# #         print(f"An error occurred: {str(e)}")

# # def validate_csv(**kwargs):
# #     print("Starting Task 3 - Validation")

# #     # Retrieve the loaded DataFrame from XCom
# #     loaded_df = kwargs['ti'].xcom_pull(task_ids='read_csv_from_url', key='loaded_df')

# #     # Perform validation logic on the loaded DataFrame
# #     # Replace this with your actual validation logic
# #     if loaded_df is not None:
# #         validation_passed = True
# #         # Example validation: Check if 'column_name' exists in the DataFrame
# #         if 'column_name' not in loaded_df.columns:
# #             validation_passed = False
# #     else:
# #         validation_passed = False

# #     print(f"CSV Validation Result: {validation_passed}")

# def end_task():
#     print("Ending Task")
#     # Perform any necessary cleanup or finalization
#     pass

# # Define the DAG
# with DAG(
#     'shyanjali_dag',
#     schedule_interval=None,
#     start_date=datetime(2023, 9, 18),  # Set your desired schedule interval
#      # Set your desired start date
#     catchup=False,
# ) as dag:

#     # Define tasks
#     start = PythonOperator(
#         task_id='start_task',
#         python_callable=start_task,
#     )

#     end = PythonOperator(
#         task_id='end_task',
#         python_callable=end_task,
#     )

#     # Define a TaskGroup for Task 2 and Task 3
#     with TaskGroup('file_processing_tasks') as file_processing_tasks:
#         read_csv = PythonOperator(
#             task_id='read_csv_from_url',
#             python_callable=read_csv_from_url,
#             provide_context=True,
#         )

#         validate_file = PythonOperator(
#             task_id='validate_csv',
#             python_callable=validate_csv,
#             provide_context=True,
#         )

#     # Set task dependencies
#     start >> file_processing_tasks>> end


  
# ---------------------------------------------------



# import pandas as pd
# from pydantic import BaseModel, ValidationError, validator
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# import requests
# from io import StringIO
# import smtplib
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart
# from email.mime.application import MIMEApplication
# from airflow.hooks.base_hook import BaseHook
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# import os

# # Define your CSV URL
# CSV_URL = 'https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv'

# default_args = {
#     'owner': 'airflow',
#     'start_date': days_ago(1),
# }

# dag = DAG(
#     'shyanjali_dag',
#     default_args=default_args,
#     schedule_interval=None,  # You can set a schedule as needed
#     catchup=False,
# )

# # Define Pydantic model for validation
# class CsvRow(BaseModel):
#     NAME: str
#     EMAIL: str
#     SSN: int
#     @validator('SSN')
#     def validate_id(cls, SSN):
#         if not (1000 <= SSN <= 9999):
#             raise ValueError('ID must be a 4-digit integer')
#         return SSN

# def fetch_and_validate_csv():
    
#     response = requests.get(CSV_URL)
#     response.raise_for_status()

#     # Read CSV data into a DataFrame
#     df = pd.read_csv(StringIO(response.text))
#     # print(df)
#     valid_rows=[]
#     # Replace with your Snowflake schema and table name
#     try:
#         # Fetch data from CSV URL
        
#         # Iterate through rows and validate each one
#         errors = []
#         for index, row in df.iterrows():
#             try:
#                 validated_row=CsvRow(**row.to_dict())
#                 print(validated_row)
#                 valid_rows.append((validated_row.NAME, validated_row.EMAIL, validated_row.SSN))
#             except ValidationError as e:
#                 # Record validation errors and add them to the "error" column
#                 error_message = f"CHECK SSN IT SHOULD HAVE 4 DIGIT NUMBER: {str(e)}"
#                 errors.append(error_message)
#                 df.at[index, 'error'] = error_message
#         # Add a new column to the DataFrame if not already present
#         if 'error' not in df.columns:
#             df['error'] = "--"
            
#         df['error'].fillna(value='--', inplace=True)
#         df['SSN'].fillna(value=0, inplace=True)
#         # df.to_csv('/tmp/data.csv', index=False)
#         csv_file_path = "/tmp/error.csv"
#         df.to_csv(csv_file_path, index=False)
#         # Export the DataFrame to CSV
#         kwargs['ti'].xcom_push(key='csv_file_path', value=csv_file_path)
        
#         # snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_li')
#         # schema = 'PUBLIC'
        
#         # connection.close()
        
#         print(f"CSV at {CSV_URL} has been validated successfully.")
        
    
#     except Exception as e:
        
#         print(f"Error: {str(e)}")
        
#     snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_li')
#     schema = 'PUBLIC'
    
#     table_name = 'SAMPLE_CSV'
#     connection = snowflake_hook.get_conn()
#     snowflake_hook.insert_rows(table_name, valid_rows)
    
#     table_name = 'SAMPLE_CSV_ERROR'
#     connection = snowflake_hook.get_conn()
#     snowflake_hook.insert_rows(table_name, df.values.tolist())
#     connection.close()
    
# def send_email_task(**kwargs):
#     csv_file_path = kwargs['ti'].xcom_pull(task_ids='fetch_and_validate_csv', key='csv_file_path')
#     email_content = "Errors in csv uploaded"
#     # Use BaseHook to get the connection
#     connection = BaseHook.get_connection('EMAIL_LI')

#     # Access connection details
#     smtp_server = connection.host
#     smtp_port = connection.port
#     smtp_username = connection.login
#     smtp_password = connection.password
#     sender_email = 'shyanjali.kantumuchu@exusia.com'

#     # recipient_email = ['charishma.jetty@exusia.com','harsha.vardhan@exusia.com']
#     recipient_email=['shyanjali.kantumuchu@exusia.com']

#     # Email details
#     email_subject = "ERROR IN CSV UPLOADED"
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
#             <p>Check the file that you have uploaded. The errors are given to the CSV attached.</p>
#             <p>Please find the attached CSV file.</p>
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
#         # Add text to the email (optional)
#         message.attach(MIMEText(email_body, 'html'))
#         # message.attach(MIMEText('Please find the attached CSV file.', 'plain'))
    
#         # Attach the CSV file
#         csv_file_path = '/tmp/error.csv'
#         with open(csv_file_path, 'rb') as file:
#             csv_attachment = MIMEApplication(file.read(), Name=os.path.basename(csv_file_path))
#         csv_attachment['Content-Disposition'] = f'attachment; filename="{os.path.basename(csv_file_path)}"'
#         message.attach(csv_attachment)
    
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

    
    
# fetch_and_validate_csv = PythonOperator(
#     task_id='fetch_and_validate_csv',
#     python_callable=fetch_and_validate_csv,
#     provide_context=True,
#     dag=dag,
# )

# send_email_task = PythonOperator(
#     task_id='send_email_task',
#     python_callable=send_email_task,
#     dag=dag,
# )

# fetch_and_validate_csv >>send_email_task







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
