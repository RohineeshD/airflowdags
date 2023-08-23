# Step 1: Importing Modules
# To initiate the DAG Object
from airflow import DAG
# Importing datetime and timedelta modules for scheduling the DAGs
from datetime import timedelta, datetime
# Importing operators 
from airflow.operators.dummy_operator import DummyOperator

def add_numbers(**kwargs):
    num1 = kwargs['num1']
    num2 = kwargs['num2']
    result = num1 + num2
    print(f"Addition Result: {num1} + {num2} = {result}")

def multiply_numbers(**kwargs):
    num1 = kwargs['num1']
    num2 = kwargs['num2']
    result = num1 * num2
    print(f"Multiplication Result: {num1} * {num2} = {result}")

# Step 2: Initiating the default_args
default_args = {
        'owner' : 'airflow',
        'start_date' : datetime(2022, 11, 12),

}

# Step 3: Creating DAG Object
dag = DAG(dag_id='shyanjali_dag',
        default_args=default_args,
        schedule_interval='@once', 
        catchup=False
    )

with DAG('math_operations_dag', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    task_addition = PythonOperator(
        task_id='addition_task',
        python_callable=add_numbers,
        op_args=[{'num1': 5, 'num2': 7}],
        provide_context=True,
    )

    task_multiplication = PythonOperator(
        task_id='multiplication_task',
        python_callable=multiply_numbers,
        op_args=[{'num1': 3, 'num2': 4}],
        provide_context=True,
    )

    task_addition >> task_multiplication


# Step 4: Creating task
# Creating first task
# start = DummyOperator(task_id = 'start', dag = dag)
# Creating second task 
# end = DummyOperator(task_id = 'end', dag = dag)

 # Step 5: Setting up dependencies 
# start >> end 
