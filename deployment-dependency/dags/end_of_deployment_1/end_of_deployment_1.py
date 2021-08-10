from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.postgres_operator import PostgresOperator

def pg_task(query_name):
    conn = S3Hook.get_connection(conn_id='s3_default')
    return PostgresOperator(
        task_id=query_name,
        sql="sql/{0}.sql".format(query_name),
        postgres_conn_id='my_data_warehouse',
        params={'login': conn.login, 'password': conn.password}
    )

with DAG(dag_id='end_of_deployment_1', start_date=datetime(2021, 8, 9), schedule_interval=None, max_active_runs=1, catchup=False) as dag:

    start = DummyOperator(task_id='start')
    write_value = pg_task('trigger_next')
    finish = DummyOperator(task_id='finish')

    start >> write_value >> finish