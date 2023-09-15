from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd

# Define your DAG
dag1 = DAG('produce_csv_link_dag', start_date=datetime(2023, 1, 1), schedule_interval=None)

# Python function to produce the CSV file link
def produce_csv_link():
    csv_link = "https://raw.githubusercontent.com/jcharishma/my.repo/master/sample_csv.csv"
    return csv_link

# Use PythonOperator to execute the function
produce_csv_link_task = PythonOperator(
    task_id='produce_csv_link',
    python_callable=produce_csv_link,
    dag=dag1,
)



# Define your DAG
dag2 = DAG('process_csv_file_dag', start_date=datetime(2023, 1, 1), schedule_interval=None)

# Python function to process the CSV file
def process_csv_file(**kwargs):
    ti = kwargs['ti']
    csv_link = ti.xcom_pull(task_ids='produce_csv_link', key=None)
    
    # Use pandas to read the CSV file
    df = pd.read_csv(csv_link, encoding='utf-8')

    print(df.head())

# Use PythonOperator to execute the function
process_csv_file_task = PythonOperator(
    task_id='process_csv_file',
    python_callable=process_csv_file,
    provide_context=True,
    dag=dag2,
)



# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime
# import pandas as pd

# # Define your DAG
# dag = DAG('df_transfer_dag', start_date=datetime(2023, 1, 1), schedule_interval=None)

# #  produce the CSV file link in DAG1
# def produce_csv_link_dag1():
#     csv_link = "https://raw.githubusercontent.com/jcharishma/my.repo/master/sample_csv.csv"
#     return csv_link

# # Python function to process the CSV file  in DAG2
# def process_csv_file_dag2(**kwargs):
#     # Retrieve the CSV file link produced by DAG1
#     ti = kwargs['ti']
#     csv_link = ti.xcom_pull(task_ids='produce_csv_link_dag1', key=None)
    
#     # Use pandas to read the CSV file
#     # df = pd.read_csv(csv_link)
#     df = pd.read_csv(csv_link, encoding='utf-8')

#     print(df.head())

# # Use PythonOperator to execute the functions in DAG1 and DAG2
# produce_csv_link_dag1 = PythonOperator(
#     task_id='produce_csv_link_dag1',
#     python_callable=produce_csv_link_dag1,
#     dag=dag,
# )

# process_csv_file_dag2 = PythonOperator(
#     task_id='process_csv_file_dag2',
#     python_callable=process_csv_file_dag2,
#     provide_context=True,
#     dag=dag,
# )

# # Set up the dependencies
# produce_csv_link_dag1 >> process_csv_file_dag2




# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import pandas as pd

# def df_transfer_dag():
#     # Serialize the DataFrame and store it
#     df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['Name', 'Email', 'SSN']})
#     serialized_df = df.to_csv("https://github.com/jcharishma/my.repo/raw/master/sample_csv.csv")

#     # Deserialize the DataFrame from the shared location
#     retrieved_df = pd.read_csv("https://github.com/jcharishma/my.repo/blob/master/rep.csv")
#     # Now you have the DataFrame and can use it within the same DAG

# dag = DAG('df_transfer_dag', start_date=datetime(2023, 1, 1), schedule_interval=None) 

# transfer_task = PythonOperator(
#     task_id='df_task',
#     python_callable=df_transfer_dag,
#     dag=dag,
# )