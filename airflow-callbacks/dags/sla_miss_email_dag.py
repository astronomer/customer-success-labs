from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": "youremail@here.com",
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    "sla": timedelta(seconds=30),
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG(
    "sla_miss_email",
    start_date=datetime(2021, 8, 5),
    max_active_runs=1,
    schedule_interval=timedelta(
        minutes=2
    ),  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
    default_args=default_args,
    catchup=False,
) as dag:
    dummy_success_test = DummyOperator(
        task_id="dummy_success_test",
    )

    bash_sleep = BashOperator(
        task_id="bash_sleep",
        bash_command="sleep 30",  # Task will sleep to showcase sla_miss callback
    )

    bash_fail = BashOperator(
        task_id="bash_fail",
        retries=1,
        bash_command="exit 123",  # Task will retry before failing to showcase on_retry_callback
    )

    dummy_success_test >> bash_sleep >> bash_fail
