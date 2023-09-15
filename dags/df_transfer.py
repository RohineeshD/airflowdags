from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def transfer_df_between_dags():
    # Serialize the DataFrame and store it
    df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['A', 'B', 'C']})
    serialized_df = df.to_csv("https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv")

    # Deserialize the DataFrame from the shared location
    retrieved_df = pd.read_csv("https://github.com/jcharishma/my.repo/blob/master/rep.csv")
    # Now you have the DataFrame and can use it within the same DAG

dag = DAG('transfer_df_between_dags', start_date=datetime(2023, 1, 1))

transfer_task = PythonOperator(
    task_id='transfer_df',
    python_callable=transfer_df_between_dags,
    dag=dag,
)
