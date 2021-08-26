from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime


def _my_python_task():
    ct = datetime.now()
    print(f"Hello World, here is the current timestamp: {ct}")

with DAG(
    "triggered_by_api",
    start_date=datetime(2021, 7, 20),
    max_active_runs=1,
    schedule_interval=None
) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    t = PythonOperator(
        task_id='my_python_task',
        python_callable=_my_python_task
    )

    start >> t >> end

