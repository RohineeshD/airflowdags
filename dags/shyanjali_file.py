from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define the functions for tasks
def multiply_numbers(**kwargs):
    num1 = kwargs['num1']
    num2 = kwargs['num2']
    result = num1 * num2
    print(f"Multiplication Result: {result}")

def add_numbers(**kwargs):
    num1 = kwargs['num1']
    num2 = kwargs['num2']
    result = num1 + num2
    print(f"Addition Result: {result}")

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 23),
    'retries': 1,
}

# Create the DAG instance
dag = DAG(
    'shyanjali',
    default_args=default_args,
    schedule_interval=None,  # Set to a specific interval (e.g., '0 0 * * *') if needed
)

# Define the tasks
num1 = 5
num2 = 3

multiply_task = PythonOperator(
    task_id='multiply_task',
    python_callable=multiply_numbers,
    op_args=[num1, num2],
    provide_context=True,
    dag=dag,
)

add_task = PythonOperator(
    task_id='add_task',
    python_callable=add_numbers,
    op_args=[num1, num2],
    provide_context=True,
    dag=dag,
)

# Set task dependencies
multiply_task >> add_task
