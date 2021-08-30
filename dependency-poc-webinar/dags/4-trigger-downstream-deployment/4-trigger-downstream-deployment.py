from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

from utils.airflow_api_tools import AirflowStableAPI

amateur_cosmos_2865 = AirflowStableAPI('amateur-cosmos-2865')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(dag_id='4-trigger-downstream-deployment',
         start_date=datetime(2021, 1, 1),
         max_active_runs=1,
         schedule_interval=None,
         catchup=False
         ) as dag:

    start = DummyOperator(task_id='start')

    trigger_dependent_dag = amateur_cosmos_2865.trigger_dag(dag_id='triggered_by_api')

    finish = DummyOperator(task_id='finish')

    start >> trigger_dependent_dag >> finish