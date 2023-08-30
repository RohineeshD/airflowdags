from airflow import DAG
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = dict(
    start_date= datetime(2021, 1, 1),
    owner="airflow",
    retries=1,
)

dag_args = dict(
    dag_id="Shyanjali_dag1",
    schedule_interval='@once',
    default_args=default_args,
    catchup=False,
)

def fetch_csv_and_upload(**kwargs):
    url = "https://raw.githubusercontent.com/cs109/2014_data/master/countries.csv"
    response = requests.get(url)
    data = response.text
    df = pd.read_csv(StringIO(data))
    # Upload DataFrame to Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_li')
    # Replace with your Snowflake schema and table name
    schema = 'PUBLIC'
    table_name = 'STAGING_TABLE'
    connection = snowflake_hook.get_conn()
    snowflake_hook.insert_rows(table_name, df.values.tolist())
    connection.close()


with DAG(**dag_args) as dag:
    # first task declaration
    fetch_and_upload = PythonOperator(
        task_id='fetch_and_upload',
        python_callable=fetch_csv_and_upload,
        provide_context=True,
        op_kwargs={},# This is required to pass context to the function
    )

# trigger_dag2 = TriggerDagRunOperator(
#     task_id='trigger_dag2',
#     trigger_dag_id="Shyanjali_dag2",
#     dag=Shyanjali_dag1,
# )

fetch_and_upload
# >> trigger_dag2
