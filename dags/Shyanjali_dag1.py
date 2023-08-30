from airflow import DAG
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_data_to_staging',
    default_args=default_args,
    schedule_interval=None,  # Set the schedule interval as needed
    catchup=False,
)

load_to_staging = S3ToSnowflakeOperator(
    task_id='load_data_to_staging',
    schema='PUBLIC',
    table='<your_staging_table>',
    stage='<your_snowflake_stage>',
    file_format='<your_snowflake_file_format>',
    s3_bucket='<your_s3_bucket>',
    s3_key='<your_s3_key>',
    aws_conn_id='<your_aws_connection_id>',
    snowflake_conn_id='<your_snowflake_connection_id>',
    dag=dag,
)

load_to_staging
