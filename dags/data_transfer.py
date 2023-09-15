from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def data_transfer_dag():
    # Serialize the DataFrame and store it
    df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['Name', 'Email', 'SSN']})
    serialized_df = df.to_csv("https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv")

    # Deserialize the DataFrame from the shared location
    retrieved_df = pd.read_csv("https://github.com/jcharishma/my.repo/blob/master/rep.csv")
    # Now you have the DataFrame and can use it within the same DAG

dag = DAG('data_transfer_dag', start_date=datetime(2023, 1, 1))

transfer_task = PythonOperator(
    task_id='data_transfer_task',
    python_callable=transfer_df_dags,
    dag=dag,
)
