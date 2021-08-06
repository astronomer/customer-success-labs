from airflow import DAG
import datetime
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from util import slack_callback_functions

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=5),
    'on_success_callback': slack_callback_functions.success_callback,
    'on_failure_callback': slack_callback_functions.failure_callback,
    'on_retry_callback': slack_callback_functions.retry_callback,
    'sla': timedelta(seconds=10)
}
with DAG(
        dag_id="slack_callbacks",
        default_args=default_args,
        start_date=datetime.datetime(2021, 7, 26),
        schedule_interval=timedelta(minutes=2),
        sla_miss_callback=slack_callback_functions.sla_miss_callback,
        catchup=False
) as dag:
    dummy_trigger = DummyOperator(
        task_id='dummy_trigger',
        on_execute_callback=slack_callback_functions.dag_triggered_callback,
        on_success_callback=None
    )

    dummy_success_test = DummyOperator(
        task_id='dummy_success_test',
    )

    slack_test_func = PythonOperator(
        task_id="slack_test_func",
        python_callable=slack_callback_functions.slack_test,
        on_success_callback=None
    )

    bash_sleep = BashOperator(
        task_id='bash_sleep',
        bash_command='sleep 30'  # Task will sleep to showcase sla_miss callback
    )

    bash_fail = BashOperator(
        task_id='bash_fail',
        retries=1,
        bash_command='abc'  # Task will retry before failing to showcase on_retry_callback
    )

    dummy_dag_success = DummyOperator(
        task_id='dummy_dag_success',
        on_success_callback=slack_callback_functions.dag_success_callback,
        trigger_rule='all_done'  # Task will still succeed despite previous task failing

    )

    (dummy_trigger >> dummy_success_test >> slack_test_func >> bash_sleep >> bash_fail >> dummy_dag_success)
