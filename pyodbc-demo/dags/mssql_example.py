# this is not production code. just useful for testing connectivity.
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def sample_select():
    odbc_hook = OdbcHook()
    cnxn = odbc_hook.get_conn()

    cursor = cnxn.cursor()
    cursor.execute("SELECT @@SERVERNAME, @@VERSION;")
    row = cursor.fetchone()
    while row:
        print("Server Name:" + row[0])
        print("Server Version:" + row[1])
        row = cursor.fetchone()

with DAG(dag_id="mssql_example", schedule_interval=None, start_date=days_ago(2), tags=["example"]) as dag:

    PythonOperator(
        task_id="sample_select",
        python_callable=sample_select,
    )