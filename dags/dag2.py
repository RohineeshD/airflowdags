# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime
# import pandas as pd

# # Define your DAG
# dag2 = DAG('process_csv_file_dag', start_date=datetime(2023, 1, 1), schedule_interval=None)

# # Python function to process the CSV file
# def process_csv_file(**kwargs):
#     ti = kwargs['ti']
#     csv_link = ti.xcom_pull(task_ids='produce_csv_link', key=None)
    
#     # Use pandas to read the CSV file
#     df = pd.read_csv(csv_link, encoding='utf-8')

#     print(df.head())

# # Use PythonOperator to execute the function
# process_csv_file_task = PythonOperator(
#     task_id='process_csv_file',
#     python_callable=process_csv_file,
#     provide_context=True,
#     dag=dag2,
# )
