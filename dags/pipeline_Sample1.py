import os
from datetime import datetime

import pytest

from airflow import DAG

try:
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
    from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
except ImportError:
    pytest.skip("MSSQL provider not available", allow_module_level=True)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_mssql"


with DAG(
    DAG_ID,
    schedule="@daily",
    start_date=datetime(2021, 10, 1),
    tags=["example"],
    catchup=False,
) as dag:

    # Example of creating a task to create a table in MsSql

    create_table_mssql_task = MsSqlOperator(
        task_id="create_country_table",
        mssql_conn_id="airflow_mssql",
        sql=r"""
        CREATE TABLE Country (
            country_id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
            name TEXT,
            continent TEXT
        );
        """,
        dag=dag,
    )

    @dag.task(task_id="insert_mssql_task")
    def insert_mssql_hook():
        mssql_hook = MsSqlHook(mssql_conn_id="airflow_mssql", schema="demo")

        rows = [
            ("India", "Asia"),
            ("Germany", "Europe"),
            ("Argentina", "South America"),
            ("Ghana", "Africa"),
            ("Japan", "Asia"),
            ("Namibia", "Africa"),
        ]
        target_fields = ["name", "continent"]
        mssql_hook.insert_rows(table="Country", rows=rows, target_fields=target_fields)
    
    get_all_countries = MsSqlOperator(
        task_id="get_all_countries",
        mssql_conn_id="airflow_mssql",
        sql=r"""SELECT * FROM Country;""",
    )
    get_countries_from_continent = MsSqlOperator(
        task_id="get_countries_from_continent",
        mssql_conn_id="airflow_mssql",
        sql=r"""SELECT * FROM Country where {{ params.column }}='{{ params.value }}';""",
        params={"column": "CONVERT(VARCHAR, continent)", "value": "Asia"},
    )
    (
        create_table_mssql_task
        >> insert_mssql_hook()
        >> get_all_countries
        >> get_countries_from_continent
    )