from airflow.models import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup


default_args = {'start_date': datetime(2021, 1, 1)}

with DAG('basic_task_groups', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    t0 = DummyOperator(task_id='start')

    # Start Task Group definition
    with TaskGroup(group_id='group1') as tg1:
        t1 = DummyOperator(task_id='task1')
        t2 = DummyOperator(task_id='task2')

        t1 >> t2
    # End Task Group definition

    t3 = DummyOperator(task_id='end')

    # Set Task Group's (tg1) dependencies
    t0 >> tg1 >> t3