from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime

# Define your default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 1),
    'retries': 1,
}

# Create a DAG instance
dag = DAG(
    'shyanjali_email_dag',
    default_args=default_args,
    schedule_interval='@once',  # You can set the schedule interval as needed
    catchup=False,
)

# Define your tasks
start_task = DummyOperator(task_id='start_task', dag=dag)

# Define a PythonOperator task that simulates success
def success_task():
    print("Success task executed successfully")

success_task = PythonOperator(
    task_id='success_task',
    python_callable=success_task,
    dag=dag,
)

# Define a PythonOperator task that simulates failure
def failure_task():
    raise Exception("Failure task failed intentionally")

failure_task = PythonOperator(
    task_id='failure_task',
    python_callable=failure_task,
    dag=dag,
)

# Define the email notification task
send_email_task = EmailOperator(
    task_id='send_email',
    to=['shyanjali47@gmail.com'],  # List of email recipients
    subject='Airflow DAG Status Email',
    html_content='The Airflow DAG has completed successfully.',
    mime_charset='utf-8',  # Set the character encoding
    files=None,  # Attach files if needed
    cc=None,  # Add CC recipients if needed
    bcc=None,  # Add BCC recipients if needed
    mime_subtype='mixed',  # Use 'mixed' to include both text and HTML content
    conn_id='EMAIL_LI',  # Specify the SMTP connection ID
    dag=dag,
)

# Set task dependencies
start_task >> success_task
start_task >> failure_task

# Send the email on success
success_task >> send_email_task

# Send the email on failure
failure_task >> send_email_task
