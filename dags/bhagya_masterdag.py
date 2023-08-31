
from airflow.operators.trigger_dagrum import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from io import StringIO
from airflow import DAG
import os
import logging
import requests
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime

default_args = {
    'owner' : 'airflow',
    'start_date' :days_ago(2)
}

def starttask():
    print('Log print Start');

def endtask():
    print('Log print End');

with DAG(dag_id='bhagya_masterdag',
        default_args=default_args,
        schedule_interval='@once', 
        catchup=False
    ) as dag:

    task1_start = PythonOperator(
        task_id="Start",
        python_callable=starttask
    )

    task2_dag1_run = TriggerDagRunOperator(
    task_id = 'DAG1',
    trigger_dag_id = 'bhagya_dag1',
    execution_date = '{{ ds }}',
    reset_dag_run = True,
    wait_for_completion =True,
    poke_interval = 3
    )

    task3_dag2_run = TriggerDagRunOperator(
    task_id = 'DAG2',
    trigger_dag_id = 'bhagya_dag2',
    execution_date = '{{ ds }}',
    reset_dag_run = True,
    wait_for_completion =True,
    poke_interval = 3
    )

    task4_end = PythonOperator(
        task_id="End",
        python_callable=endtask
    )

task1_start >> task2_dag1_run >> task3_dag2_run >> task4_end
