from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Specify the connection ID you want to retrieve details for
connection_id = 'EMAIL_LI'

# Function to send an email using SMTP connection details
def send_email():
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
    email_subject = "Airflow Email Notification"
    email_body = "This is a test 2 email from Airflow using a connection."

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
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 1),
    'retries': 1,
}

dag = DAG(
    'shyanjali_send_email',
    default_args=default_args,
    schedule_interval='@once',  # Set your desired schedule interval
    catchup=False,
)

# Task to send the email using the defined function
send_email_task = PythonOperator(
    task_id='send_email_task',
    python_callable=send_email,
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()
